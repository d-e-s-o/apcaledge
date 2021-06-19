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
    let (req, activities, remainder) = activites_for_a_day(client, unprocessed, request).await?;
    if activities.is_empty() {
      assert!(remainder.is_empty());
      break
    }

    request = req;
    unprocessed = remainder;

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
