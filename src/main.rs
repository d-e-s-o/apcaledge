// Copyright (C) 2020-2021 Daniel Mueller <deso@posteo.net>
// SPDX-License-Identifier: GPL-3.0-or-later

use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::fs::File;
use std::io::stdout;
use std::io::Write;
use std::path::PathBuf;
use std::process::exit;
use std::time::SystemTime;
use std::time::SystemTimeError;
use std::time::UNIX_EPOCH;

use apca::api::v2::account;
use apca::api::v2::account_activities;
use apca::ApiInfo;
use apca::Client;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;

use chrono::offset::TimeZone;
use chrono::offset::Utc;
use chrono::DateTime;

use num_decimal::Num;

use serde_json::from_reader as json_from_reader;

use structopt::StructOpt;

use time_util::parse_system_time_from_date_str;

use tokio::runtime::Builder;

use tracing::subscriber::set_global_default as set_global_subscriber;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::time::ChronoLocal;
use tracing_subscriber::FmtSubscriber;

const ALPACA: &str = "Alpaca Securities LLC";
const DEFAULT_INVESTMENT_ACCOUNT: &str = "Assets:Investments:Alpaca:Stock";
const DEFAULT_BROKERAGE_ACCOUNT: &str = "Assets:Alpaca Brokerage";
const DEFAULT_BROKERAGE_FEE_ACCOUNT: &str = "Expenses:Broker:Fee";
const DEFAULT_DIVIDEND_ACCOUNT: &str = "Income:Dividend";
const DEFAULT_SEC_FEE_ACCOUNT: &str = "Expenses:Broker:SEC Fee";
const DEFAULT_FINRA_TAF_ACCOUNT: &str = "Expenses:Broker:FINRA TAF";


/// Parse a `SystemTime` from a provided date.
fn parse_date(date: &str) -> Result<SystemTime> {
  parse_system_time_from_date_str(date).ok_or_else(|| anyhow!("{} is not a valid date", date))
}


/// A command line client for formatting Alpaca trades in Ledger format.
#[derive(Debug, StructOpt)]
struct Opts {
  /// The path to the JSON registry for looking up names from symbols.
  registry: PathBuf,
  /// Only show activities dated at the given date or after (format:
  /// yyyy-mm-dd).
  #[structopt(short, long, parse(try_from_str = parse_date))]
  begin: Option<SystemTime>,
  /// The name of the investment account, i.e., the one holding the
  /// shares.
  #[structopt(long, default_value = DEFAULT_INVESTMENT_ACCOUNT)]
  investment_account: String,
  /// The name of the brokerage account, i.e., the one holding any
  /// uninvested cash.
  #[structopt(long, default_value = DEFAULT_BROKERAGE_ACCOUNT)]
  brokerage_account: String,
  /// The name of the brokerage's fee account.
  #[structopt(long, default_value = DEFAULT_BROKERAGE_FEE_ACCOUNT)]
  brokerage_fee_account: String,
  /// The name of the account to account dividend payments against.
  #[structopt(long, default_value = DEFAULT_DIVIDEND_ACCOUNT)]
  dividend_account: String,
  /// The name of the account to use for regulatory fees by the SEC.
  #[structopt(long, default_value = DEFAULT_SEC_FEE_ACCOUNT)]
  sec_fee_account: String,
  /// The name of the account to use for FINRA trade activity fees.
  #[structopt(long, default_value = DEFAULT_FINRA_TAF_ACCOUNT)]
  finra_taf_account: String,
  /// Increase verbosity (can be supplied multiple times).
  #[structopt(short = "v", long = "verbose", global = true, parse(from_occurrences))]
  verbosity: usize,
}


/// Format a price value.
fn format_price(price: &Num, currency: &str) -> String {
  // We would like to ensure emitting prices with at least two post
  // decimal positions, for consistency.
  format!("{} {}", price.display().min_precision(2), currency)
}

/// Convert a `SystemTime` into a `DateTime`.
fn convert_time(time: &SystemTime) -> Result<DateTime<Utc>, SystemTimeError> {
  time.duration_since(UNIX_EPOCH).map(|duration| {
    let secs = duration.as_secs().try_into().unwrap();
    let nanos = duration.subsec_nanos();
    let time = Utc.timestamp(secs, nanos);
    time
  })
}

/// Format a system time as a date.
fn format_date(time: &SystemTime) -> String {
  convert_time(time)
    .map(|time| time.date().format("%Y-%m-%d").to_string().into())
    .unwrap()
}

fn print_trade(
  trade: &account_activities::TradeActivity,
  investment_account: &str,
  brokerage_account: &str,
  registry: &HashMap<String, String>,
  currency: &str,
) -> Result<()> {
  let name = registry
    .get(&trade.symbol)
    .ok_or_else(|| anyhow!("symbol {} not present in registry", trade.symbol))?;

  let multiplier = match trade.side {
    account_activities::Side::Buy => 1,
    account_activities::Side::Sell => -1,
    account_activities::Side::ShortSell => -1,
  };

  println!(r#"{date} * {name}
  {from:<51}  {qty:>13} {sym} @ {price}
  {to:<51}    {total:>15}
"#,
    date = format_date(&trade.transaction_time),
    name = name,
    from = investment_account,
    to = brokerage_account,
    qty = trade.quantity as i32 * multiplier,
    sym = trade.symbol,
    price = format_price(&trade.price, &currency),
    total = format_price(
      &(&trade.price * trade.quantity as i32 * -multiplier),
      &currency
    ),
  );
  Ok(())
}

fn print_non_trade(
  non_trade: &account_activities::NonTradeActivity,
  brokerage_account: &str,
  brokerage_fee_account: &str,
  dividend_account: &str,
  sec_fee_account: &str,
  finra_taf_account: &str,
  registry: &HashMap<String, String>,
  currency: &str,
) -> Result<()> {
  match non_trade.type_ {
    account_activities::ActivityType::Dividend => {
      let symbol = non_trade
        .symbol
        .as_ref()
        .ok_or_else(|| anyhow!("dividend entry does not have an associated symbol"))?;
      let name = registry
        .get(symbol)
        .ok_or_else(|| anyhow!("symbol {} not present in registry", symbol))?;

      println!(
        r#"{date} * {name}
  {from:<51}
  {to:<51}    {total:>15}
"#,
        date = format_date(&non_trade.date),
        name = name,
        from = dividend_account,
        to = brokerage_account,
        total = format_price(&non_trade.net_amount, currency),
      );
    },
    account_activities::ActivityType::PassThruCharge => {
      let desc = non_trade
        .description
        .as_ref()
        .map(|desc| format!("\n  ; {}", desc).into())
        .unwrap_or_else(|| Cow::from(""));

      println!(
        r#"{date} * {name}{desc}
  {from:<51}
  {to:<51}    {total:>15}
"#,
        date = format_date(&non_trade.date),
        name = ALPACA,
        desc = desc,
        from = brokerage_fee_account,
        to = brokerage_account,
        total = format_price(&non_trade.net_amount, currency),
      );
    },
    account_activities::ActivityType::Fee => {
      let desc = non_trade
        .description
        .as_ref()
        .map(String::as_ref)
        .unwrap_or_else(|| "");

      let to = if desc.starts_with("TAF fee") {
        finra_taf_account
      } else if desc.starts_with("REG fee") {
        sec_fee_account
      } else {
        bail!(
          "failed to classify fee account activity with description: {}",
          desc
        )
      };

      println!(
        r#"{date} * {name}
  ; {desc}
  {to:<51}    {total:>15}
"#,
        date = format_date(&non_trade.date),
        name = ALPACA,
        desc = desc,
        to = to,
        total = format_price(&-&non_trade.net_amount, currency),
      );
    },
    _ => (),
  }
  Ok(())
}


/// Retrieve the time stamp at which an account activity occurred.
fn time(activity: &account_activities::Activity) -> SystemTime {
  match activity {
    account_activities::Activity::Trade(trade) => trade.transaction_time,
    account_activities::Activity::NonTrade(non_trade) => non_trade.date,
  }
}


/// Retrieve account activities spanning at least one day.
async fn activites_for_a_day(
  client: &mut Client,
  mut activities: VecDeque<account_activities::Activity>,
  mut request: account_activities::ActivityReq,
) -> Result<(
  account_activities::ActivityReq,
  VecDeque<account_activities::Activity>,
  VecDeque<account_activities::Activity>,
)> {
  loop {
    if let Some(last) = activities.back() {
      // If we have a last element we must have a first one, so it's
      // fine to unwrap.
      let first = activities.front().unwrap();
      // TODO: We should use apca::account_activities::Activity::time
      //       once it's released.
      let start = DateTime::<Utc>::from(time(first)).date();
      let end = DateTime::<Utc>::from(time(last)).date();

      if start != end {
        // The date changed between the first and the last activity,
        // meaning that we encountered activities for another day. As
        // such, report the activities collected so far.
        let (same_day, other_day) = activities
          .into_iter()
          .partition(|activity| DateTime::<Utc>::from(time(activity)).date() == start);

        break Ok((request, same_day, other_day))
      }
    }

    let fetched = client
      .issue::<account_activities::Get>(&request)
      .await
      .with_context(|| "failed to retrieve account activities")?;

    if let Some(last) = fetched.last() {
      // If we retrieved some data make sure to update the page token
      // such that the next request will be for data past what we just
      // got.
      request.page_token = Some(last.id().to_string());
      activities.append(&mut VecDeque::from(fetched));
    } else {
      // We reached the end of the activity "stream", as nothing else
      // was reported.
      break Ok((request, activities, VecDeque::new()))
    }
  }
}


/// Merge partial fills for the same order at the same price.
fn merge_partial_fills(
  mut activities: VecDeque<account_activities::Activity>,
) -> VecDeque<account_activities::Activity> {
  let mut i = 0;
  'outer: while i < activities.len() {
    if let account_activities::Activity::Trade(trade) = &activities[i] {
      if (trade.unfilled_quantity != 0 &&
        // If `cumulative_quantity` equals `quantity` it would be the
        // first partial fill, in which case there is nothing to merge
        // yet.
        trade.cumulative_quantity != trade.quantity) ||
        // We can't differentiate between a fill in one go and the
        // last partial fill that completes an order. As such, attempt
        // a merge in both cases.
        trade.unfilled_quantity == 0
      {
        // See if we can merge the trade with an earlier one.
        for j in 0..i {
          if let account_activities::Activity::Trade(candidate) = &activities[j] {
            if candidate.order_id == trade.order_id && candidate.price == trade.price {
              debug_assert_eq!(candidate.side, trade.side);
              debug_assert_eq!(candidate.symbol, trade.symbol);
              debug_assert!(candidate.unfilled_quantity >= trade.quantity);

              let time = trade.transaction_time;
              let quantity = trade.quantity;

              if let account_activities::Activity::Trade(candidate) = &mut activities[j] {
                candidate.transaction_time = time;
                candidate.quantity += quantity;
                candidate.unfilled_quantity -= quantity;
                candidate.cumulative_quantity += quantity;

                // Remove the outer trade activity. We do not increment
                // `i` on this path, so we handle the removal correctly.
                activities.remove(i);
                continue 'outer
              } else {
                unreachable!()
              }
            }
          }
        }
      }
    }

    i += 1;
  }

  activities
}


async fn activities_list(
  client: &mut Client,
  begin: Option<SystemTime>,
  investment_account: &str,
  brokerage_account: &str,
  brokerage_fee_account: &str,
  dividend_account: &str,
  sec_fee_account: &str,
  finra_taf_account: &str,
  registry: &HashMap<String, String>,
) -> Result<()> {
  let mut unprocessed = VecDeque::new();
  let mut request = account_activities::ActivityReq {
    direction: account_activities::Direction::Ascending,
    after: begin,
    ..Default::default()
  };

  let currency = client
    .issue::<account::Get>(&())
    .await
    .with_context(|| "failed to retrieve account information")?
    .currency;

  loop {
    let (req, mut activities, remainder) =
      activites_for_a_day(client, unprocessed, request).await?;
    if activities.is_empty() {
      assert!(remainder.is_empty());
      break
    }

    request = req;
    unprocessed = remainder;

    activities = merge_partial_fills(activities);

    for activity in activities {
      match activity {
        account_activities::Activity::Trade(trade) => print_trade(
          &trade,
          investment_account,
          brokerage_account,
          registry,
          &currency,
        )?,
        account_activities::Activity::NonTrade(non_trade) => print_non_trade(
          &non_trade,
          brokerage_account,
          brokerage_fee_account,
          dividend_account,
          sec_fee_account,
          finra_taf_account,
          registry,
          &currency,
        )?,
      }
    }
  }
  Ok(())
}

async fn run() -> Result<()> {
  let opts = Opts::from_args();
  let level = match opts.verbosity {
    0 => LevelFilter::WARN,
    1 => LevelFilter::INFO,
    2 => LevelFilter::DEBUG,
    _ => LevelFilter::TRACE,
  };

  let subscriber = FmtSubscriber::builder()
    .with_max_level(level)
    .with_timer(ChronoLocal::rfc3339())
    .finish();

  set_global_subscriber(subscriber).with_context(|| "failed to set tracing subscriber")?;

  let file = File::open(&opts.registry)
    .with_context(|| format!("failed to open registry file {}", opts.registry.display()))?;
  let registry = json_from_reader::<_, HashMap<String, String>>(file)
    .with_context(|| format!("failed to read registry {}", opts.registry.display()))?;

  let api_info =
    ApiInfo::from_env().with_context(|| "failed to retrieve Alpaca environment information")?;
  let mut client = Client::new(api_info);

  activities_list(
    &mut client,
    opts.begin,
    &opts.investment_account,
    &opts.brokerage_account,
    &opts.brokerage_fee_account,
    &opts.dividend_account,
    &opts.sec_fee_account,
    &opts.finra_taf_account,
    &registry,
  )
  .await
}

fn main() {
  let rt = Builder::new_current_thread().enable_io().build().unwrap();
  let exit_code = rt
    .block_on(run())
    .map(|_| 0)
    .map_err(|e| {
      eprint!("{}", e);
      e.chain().skip(1).for_each(|cause| eprint!(": {}", cause));
      eprintln!();
    })
    .unwrap_or(1);
  // We exit the process the hard way next, so make sure to flush
  // buffered content.
  let _ = stdout().flush();
  exit(exit_code)
}


#[cfg(test)]
mod tests {
  use super::*;

  use serde_json::from_str as from_json;


  /// Test merging of partial fills.
  #[test]
  fn merge_activities_simple() {
    let activities = r#"[
{"id":"11111111111111111::22222222-3333-4444-5555-666666666666","activity_type":"FILL","transaction_time":"2021-06-15T16:17:44.31Z","type":"partial_fill","price":"9.33","qty":"1","side":"sell","symbol":"XYZ","leaves_qty":"55","order_id":"12345678-9012-3456-7890-123456789012","cum_qty":"1","order_status":"partially_filled"},
{"id":"777777777777777777::88888888-9999-1111-2222-333333333333","activity_type":"FILL","transaction_time":"2021-06-15T16:18:56.299Z","type":"partial_fill","price":"9.33","qty":"1","side":"sell","symbol":"XYZ","leaves_qty":"54","order_id":"12345678-9012-3456-7890-123456789012","cum_qty":"2","order_status":"partially_filled"},
{"id":"44444444444444444::55555555-6666-7777-8888-999999999999","activity_type":"FILL","transaction_time":"2021-06-15T16:19:18.136Z","type":"fill","price":"9.33","qty":"54","side":"sell","symbol":"XYZ","leaves_qty":"0","order_id":"12345678-9012-3456-7890-123456789012","cum_qty":"56","order_status":"filled"}
]"#;
    let activities = from_json::<VecDeque<account_activities::Activity>>(activities).unwrap();
    let activities = merge_partial_fills(activities);

    assert_eq!(activities.len(), 1);
    match &activities[0] {
      account_activities::Activity::Trade(trade) => {
        assert_eq!(trade.quantity, 56);
        assert_eq!(trade.cumulative_quantity, 56);
        assert_eq!(trade.unfilled_quantity, 0);
      },
      _ => panic!("encountered unexpected account activity"),
    }
  }


  /// Test merging of partial fills with intermixed unrelated activity.
  #[test]
  fn merge_activities_complex() {
    let activities = r#"[
{"id":"11111111111111111::11111111-1111-1111-1111-111111111111","activity_type":"FILL","transaction_time":"2021-06-15T16:19:18.136Z","type":"fill","price":"9.33","qty":"54","side":"sell","symbol":"BCD","leaves_qty":"0","order_id":"00000000-0000-0000-0000-000000000000","cum_qty":"56","order_status":"filled"},
{"id":"22222222222222222::22222222-2222-2222-2222-222222222222","activity_type":"DIV","date":"2021-06-16","net_amount":"1.87","description":"Cash DIV @ 0.17, Pos QTY: 11.0, Rec Date: 2021-05-20","symbol":"EFG","qty":"11","per_share_amount":"0.17","status":"executed"},
{"id":"33333333333333333::33333333-3333-3333-3333-333333333333","activity_type":"FILL","transaction_time":"2021-06-17T15:35:39.608Z","type":"partial_fill","price":"422.5","qty":"100","side":"buy","symbol":"XYZ","leaves_qty":"75","order_id":"12345678-9123-4567-8912-345678912345","cum_qty":"100","order_status":"partially_filled"},
{"id":"44444444444444444::44444444-4444-4444-4444-444444444444","activity_type":"FILL","transaction_time":"2021-06-17T15:35:39.772Z","type":"partial_fill","price":"422.5","qty":"27","side":"buy","symbol":"XYZ","leaves_qty":"48","order_id":"12345678-9123-4567-8912-345678912345","cum_qty":"127","order_status":"partially_filled"},
{"id":"55555555555555555::55555555-5555-5555-5555-555555555555","activity_type":"FILL","transaction_time":"2021-06-17T15:35:39.776Z","type":"partial_fill","price":"422.5","qty":"27","side":"buy","symbol":"XYZ","leaves_qty":"21","order_id":"12345678-9123-4567-8912-345678912345","cum_qty":"154","order_status":"partially_filled"},
{"id":"66666666666666666::66666666-6666-6666-6666-666666666666","activity_type":"FILL","transaction_time":"2021-06-17T15:35:39.781Z","type":"fill","price":"422.5","qty":"21","side":"buy","symbol":"XYZ","leaves_qty":"0","order_id":"12345678-9123-4567-8912-345678912345","cum_qty":"175","order_status":"filled"},
{"id":"77777777777777777::77777777-7777-7777-7777-777777777777","activity_type":"DIV","date":"2021-06-18","net_amount":"8.22","description":"Cash DIV @ 0.02","symbol":"ABC","qty":"411","per_share_amount":"0.02","status":"executed"}
]"#;
    let activities = from_json::<VecDeque<account_activities::Activity>>(activities).unwrap();
    let activities = merge_partial_fills(activities);

    assert_eq!(activities.len(), 4);
    match &activities[2] {
      account_activities::Activity::Trade(trade) => {
        assert_eq!(trade.quantity, 175);
        assert_eq!(trade.cumulative_quantity, 175);
        assert_eq!(trade.unfilled_quantity, 0);
      },
      _ => panic!("encountered unexpected account activity"),
    }
  }
}
