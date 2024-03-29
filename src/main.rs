// Copyright (C) 2020-2024 Daniel Mueller <deso@posteo.net>
// SPDX-License-Identifier: GPL-3.0-or-later

#![allow(
  clippy::assign_op_pattern,
  clippy::let_and_return,
  clippy::let_unit_value,
  clippy::too_many_arguments
)]

mod args;

use std::borrow::Cow;
use std::cmp::min;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs::File;
use std::future::Future;
use std::io::stderr;
use std::io::stdout;
use std::io::Write;
use std::process::exit;
use std::str::FromStr as _;
use std::sync::Arc;

use apca::api::v2::account;
use apca::api::v2::account_activities;
use apca::api::v2::clock;
use apca::data::v2::bars;
use apca::ApiInfo;
use apca::Client;
use apca::RequestError;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;

use chrono::DateTime;
use chrono::Datelike as _;
use chrono::Duration;
use chrono::Local;
use chrono::NaiveDate;
use chrono::TimeZone as _;
use chrono::Utc;
use chrono_tz::America::New_York;

use futures::future::join;
use futures::future::ready;
use futures::future::Shared;
use futures::stream::iter;
use futures::FutureExt as _;
use futures::StreamExt as _;
use futures::TryFutureExt as _;
use futures::TryStreamExt as _;

use num_decimal::Num;

use once_cell::sync::Lazy;

use regex::Regex;

use serde_json::from_reader as json_from_reader;

use structopt::StructOpt as _;

use tokio::runtime::Builder;

use tracing::subscriber::set_global_default as set_global_subscriber;
use tracing::warn;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::FmtSubscriber;

use crate::args::Args;
use crate::args::Command;

const ALPACA: &str = "Alpaca Securities LLC";


// TODO: Presumably, with fractional shares being supported by the API
//       we need to support a floating point value here. But that likely
//       needs adjustments in `apca` as well.
static TAF_RE: Lazy<Regex> =
  Lazy::new(|| Regex::new(r"TAF fee for proceed of (?P<shares>\d+) shares").unwrap());
// TODO: It is unclear whether we can always assume a floating point
//       representation like we do here.
static REG_RE: Lazy<Regex> =
  Lazy::new(|| Regex::new(r"REG fee for proceed of \$(?P<proceeds>\d+\.\d+)").unwrap());
static ADR_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^ADR Fees").unwrap());
static ACQ_PRICE_RE: Lazy<Regex> =
  Lazy::new(|| Regex::new(r"Cash Merger \$(?P<price>\d+\.\d+)").unwrap());


/// Format a price value.
fn format_price(price: &Num, currency: &str) -> String {
  // We would like to ensure emitting prices with at least two post
  // decimal positions, for consistency.
  format!("{} {}", price.display().min_precision(2), currency)
}

/// Format a date time as a date.
fn format_date(time: DateTime<Utc>) -> String {
  time.date_naive().format("%Y-%m-%d").to_string()
}

fn print_trade(
  trade: &account_activities::TradeActivity,
  fees: &[account_activities::NonTradeActivity],
  investment_account: &str,
  brokerage_account: &str,
  brokerage_fee_account: &str,
  sec_fee_account: &str,
  finra_taf_account: &str,
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
    _ => panic!("encountered unexpected trade side: {:?}", trade.side),
  };

  println!(
    r#"{date} * {name}
  {from:<51}  {qty:>13} {sym} @ {price}"#,
    date = format_date(trade.transaction_time),
    name = name,
    from = investment_account,
    qty = &trade.quantity * multiplier,
    sym = trade.symbol,
    price = format_price(&trade.price, currency),
  );

  let mut total_fees = Num::from(0);
  for fee in fees {
    let net_amount = &-&fee.net_amount;
    let (to, description) = classify_fee(
      fee,
      brokerage_fee_account,
      sec_fee_account,
      finra_taf_account,
    )?;
    println!(
      r#"  ; {desc}
  {to:<51}    {total:>15}"#,
      desc = description,
      to = to,
      total = format_price(net_amount, currency),
    );

    total_fees += net_amount;
  }

  println!(
    "  {to:<51}    {total:>15}\n",
    to = brokerage_account,
    total = format_price(
      &(&(&trade.price * &trade.quantity * -multiplier) - total_fees),
      currency
    ),
  );
  Ok(())
}


/// Classify a non-trade fee activity according to its description.
fn classify_fee<'act, 'acc>(
  non_trade: &'act account_activities::NonTradeActivity,
  brokerage_fee_account: &'acc str,
  sec_fee_account: &'acc str,
  finra_taf_account: &'acc str,
) -> Result<(&'acc str, &'act str)> {
  debug_assert_eq!(non_trade.type_, account_activities::ActivityType::Fee);

  if let Some(description) = &non_trade.description {
    if TAF_RE.is_match(description) {
      Ok((finra_taf_account, description))
    } else if REG_RE.is_match(description) {
      Ok((sec_fee_account, description))
    } else if ADR_RE.find(description).is_some() {
      Ok((brokerage_fee_account, description))
    } else {
      bail!(
        "failed to classify fee account activity with description: {}",
        description
      )
    }
  } else {
    bail!("fee activity does not have a description")
  }
}


/// Extract the acquisition share price of a non-trade acquisition
/// activity.
fn extract_acquisition_share_price(
  non_trade: &account_activities::NonTradeActivity,
) -> Result<Num> {
  debug_assert_eq!(
    non_trade.type_,
    account_activities::ActivityType::Acquisition
  );

  let description = non_trade
    .description
    .as_ref()
    .context("acquisition activity does not have a description")?;
  let captures = ACQ_PRICE_RE
    .captures(description)
    .with_context(|| "acquisition non-trade activity description could not be parsed")?;
  let share_price = &captures["price"];
  let share_price = Num::from_str(share_price)
    .with_context(|| format!("failed to parse price string '{}' as number", share_price))?;

  Ok(share_price)
}


fn print_non_trade(
  non_trade: &account_activities::NonTradeActivity,
  investment_account: &str,
  brokerage_account: &str,
  brokerage_fee_account: &str,
  dividend_account: &str,
  sec_fee_account: &str,
  finra_taf_account: &str,
  registry: &HashMap<String, String>,
  currency: &str,
) -> Result<()> {
  match non_trade.type_ {
    account_activities::ActivityType::CashDeposit
    | account_activities::ActivityType::CashWithdrawal => {
      let desc = non_trade
        .description
        .as_ref()
        .map(|desc| format!("\n  ; {}", desc).into())
        .unwrap_or_else(|| Cow::from(""));

      println!(
        r#"{date} * Transfer{desc}
  {from:<51}    {total:>15}
  XXX
"#,
        date = format_date(non_trade.date),
        from = brokerage_account,
        total = format_price(&non_trade.net_amount, currency),
      );
    },
    account_activities::ActivityType::Interest => {
      let desc = non_trade
        .description
        .as_ref()
        .map(|desc| format!("\n  ; {}", desc).into())
        .unwrap_or_else(|| Cow::from(""));

      println!(
        r#"{date} * {name}{desc}
  Income:Interest
  {to:<51}    {total:>15}
"#,
        date = format_date(non_trade.date),
        name = ALPACA,
        to = brokerage_account,
        total = format_price(&non_trade.net_amount, currency),
      );
    },
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
  {from}
  {to:<51}    {total:>15}
"#,
        date = format_date(non_trade.date),
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
  {from}
  {to:<51}    {total:>15}
"#,
        date = format_date(non_trade.date),
        name = ALPACA,
        desc = desc,
        from = brokerage_fee_account,
        to = brokerage_account,
        total = format_price(&non_trade.net_amount, currency),
      );
    },
    account_activities::ActivityType::Fee => {
      let (from, desc) = classify_fee(
        non_trade,
        brokerage_fee_account,
        sec_fee_account,
        finra_taf_account,
      )?;
      println!(
        r#"{date} * {name}
  ; {desc}
  {from:<51}    {total:>15}
  {to}
"#,
        date = format_date(non_trade.date),
        name = ALPACA,
        desc = desc,
        from = from,
        to = brokerage_account,
        total = format_price(&-&non_trade.net_amount, currency),
      );
    },
    account_activities::ActivityType::Acquisition => {
      // Note that we have seen "acquisition" activities that have a
      // zero dollar amount and do not actually fit what we expect an
      // acquisition to look like. Given that they are for no amount, it
      // should be safe to just ignore them here.
      if non_trade.net_amount.is_zero() {
        return Ok(())
      }

      let share_price = extract_acquisition_share_price(non_trade)
        .context("failed to extract share price from acquisition activity")?;
      let symbol = non_trade
        .symbol
        .as_ref()
        .ok_or_else(|| anyhow!("acquisition entry does not have an associated symbol"))?;
      let name = registry
        .get(symbol)
        .ok_or_else(|| anyhow!("symbol {} not present in registry", symbol))?;
      let quantity = &non_trade.net_amount / &share_price;

      println!(
        r#"; {name} got acquired
{date} * {name}
  {from:<51}  {qty:>13} {symbol} @ {price} = 0 {symbol}
  {to:<51}    {total:>15}
"#,
        date = format_date(non_trade.date),
        name = name,
        symbol = symbol,
        qty = quantity,
        price = format_price(&share_price, currency),
        from = investment_account,
        to = brokerage_account,
        total = format_price(&non_trade.net_amount, currency),
      );
    },
    account_activities::ActivityType::StockSplit => {
      let symbol = non_trade
        .symbol
        .as_ref()
        .ok_or_else(|| anyhow!("stock split entry does not have an associated symbol"))?;
      let name = registry
        .get(symbol)
        .ok_or_else(|| anyhow!("symbol {} not present in registry", symbol))?;
      let price = non_trade.price.as_ref().ok_or_else(|| {
        anyhow!(
          "stock split entry for {} does not have an associated price",
          symbol
        )
      })?;
      let quantity = non_trade.quantity.as_ref().ok_or_else(|| {
        anyhow!(
          "stock split entry for {} does not have an associated quantity",
          symbol
        )
      })?;
      let description = non_trade
        .description
        .as_ref()
        .map(|description| format!("\n  ; {}", description).into())
        .unwrap_or_else(|| Cow::from(""));

      println!(
        r#"{date} * {name}
  ; Stock split{desc}
  {from:<51}  {qty:>13} {symbol} @ {price}
  {to:<51}    {total:>15}
"#,
        date = format_date(non_trade.date),
        name = name,
        desc = description,
        symbol = symbol,
        qty = quantity,
        price = format_price(price, currency),
        from = investment_account,
        to = brokerage_account,
        total = format_price(&(quantity * price), currency),
      );
    },
    _ => warn!("ignoring unsupported non-trade activity type: {non_trade:#?}"),
  }
  Ok(())
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
      let start = first.time().date_naive();
      let end = last.time().date_naive();

      if start != end {
        // The date changed between the first and the last activity,
        // meaning that we encountered activities for another day. As
        // such, report the activities collected so far.
        let (same_day, other_day) = activities
          .into_iter()
          .partition(|activity| activity.time().date_naive() == start);

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
      // If we have a trade that has unfilled quantity left (i.e., does
      // not complete an order), then we search for the matching "final"
      // fill to merge with.
      if !trade.unfilled_quantity.is_zero() {
        // See if we can merge the trade with another one. Note that
        // Alpaca may send activities in any order, really, and so we
        // cannot just look at later ones but actually have to scan the
        // entire array.
        for j in 0..activities.len() {
          if j == i {
            // We do not want to merge an activity with itself.
            continue
          }

          if let account_activities::Activity::Trade(candidate) = &activities[j] {
            // We are looking for the "final" fill, i.e., the one that
            // completes the order. It will have an `unfilled_quantity`
            // of 0.
            // Note that it is possible there there is no such fill in
            // the list of activities. That is because we process them
            // in batches and it is conceivable that not all partial
            // fills for an order happened in the same batch. So we may
            // end up missing out merging partial fills even, pushing
            // the burden on the user. That should be a rare occurrence
            // and it won't be too much work, though.
            if candidate.order_id == trade.order_id
              && candidate.price == trade.price
              && candidate.unfilled_quantity.is_zero()
            {
              debug_assert_eq!(candidate.side, trade.side);
              debug_assert_eq!(candidate.symbol, trade.symbol);

              let quantity = trade.quantity.clone();

              if let account_activities::Activity::Trade(candidate) = &mut activities[j] {
                candidate.quantity += quantity;
                debug_assert!(candidate.quantity <= candidate.cumulative_quantity);

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


/// An activity as used by the program, created by processing Alpaca
/// provided ones.
enum Activity {
  /// A trade activity with a optional associated regulatory fees.
  Trade(
    account_activities::TradeActivity,
    Vec<account_activities::NonTradeActivity>,
  ),
  /// A non-trade activity (e.g., a dividend payment).
  NonTrade(account_activities::NonTradeActivity),
}

impl From<account_activities::Activity> for Activity {
  fn from(other: account_activities::Activity) -> Self {
    match other {
      account_activities::Activity::Trade(trade) => Self::Trade(trade, Vec::new()),
      account_activities::Activity::NonTrade(non_trade) => Self::NonTrade(non_trade),
    }
  }
}

/// Try to associate (or merge) all non-trade fee activity with the
/// corresponding trades.
fn associate_fees_with_trades(
  activities: VecDeque<account_activities::Activity>,
) -> Result<VecDeque<Activity>> {
  let mut activities = activities
    .into_iter()
    .map(Activity::from)
    .collect::<VecDeque<_>>();

  let mut i = 0;
  'outer: while i < activities.len() {
    if let Activity::NonTrade(non_trade) = &activities[i] {
      if non_trade.type_ == account_activities::ActivityType::Fee {
        if let Some(description) = &non_trade.description {
          let (shares, proceeds) = if let Some(captures) = TAF_RE.captures(description) {
            let shares = &captures["shares"];
            let shares = Num::from_str(shares)
              .with_context(|| format!("failed to parse shares string '{}' as number", shares))?;
            (Some(shares), None)
          } else if let Some(captures) = REG_RE.captures(description) {
            let proceeds = &captures["proceeds"];
            let proceeds = Num::from_str(proceeds).with_context(|| {
              format!("failed to parse proceeds string '{}' as number", proceeds)
            })?;
            (None, Some(proceeds))
          } else if ADR_RE.find(description).is_some() {
            // ADR fees aren't associated with a trade, so just skip it
            // here.
            i += 1;
            continue 'outer
          } else {
            bail!("description string could not be parsed: {}", description)
          };

          let non_trade = non_trade.clone();

          // Note that we actually have to scan the entire list of
          // activities, because there is no guarantee that a fee is
          // reported strictly after the corresponding trade, apparently.
          for j in 0..activities.len() {
            if let Activity::Trade(trade, fees) = &mut activities[j] {
              if Some(&trade.quantity) == shares.as_ref()
                || Some(&trade.price * &trade.quantity) == proceeds
              {
                fees.push(non_trade);
                activities.remove(i);
                continue 'outer
              }
            }
          }
        } else {
          bail!("fee activity does not have a description")
        }
      }
    }

    i += 1;
  }

  Ok(activities)
}

async fn activities_list(
  client: &mut Client,
  begin: Option<NaiveDate>,
  force_separate_fees: bool,
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
    after: begin.map(|begin| Utc.from_utc_datetime(&begin.and_hms_opt(0, 0, 0).unwrap())),
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

    let activities = merge_partial_fills(activities);
    let activities = if force_separate_fees {
      activities
        .into_iter()
        .map(Activity::from)
        .collect::<VecDeque<_>>()
    } else {
      associate_fees_with_trades(activities)?
    };

    for activity in activities {
      match &activity {
        Activity::Trade(trade, fees) => print_trade(
          trade,
          fees,
          investment_account,
          brokerage_account,
          brokerage_fee_account,
          sec_fee_account,
          finra_taf_account,
          registry,
          &currency,
        )?,
        Activity::NonTrade(non_trade) => print_non_trade(
          non_trade,
          investment_account,
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


/// Retrieve and print the price of the asset with the given symbol.
async fn price_get<F>(
  client: &Client,
  symbol: String,
  date: NaiveDate,
  clock: Shared<F>,
) -> Result<()>
where
  F: Future<Output = Result<clock::Clock, Arc<RequestError<clock::GetError>>>>,
{
  let today = Local::now().date_naive();
  ensure!(date <= today, "the provided date needs to be in the past");

  let start = date - Duration::weeks(2);
  let start = New_York
    .with_ymd_and_hms(start.year(), start.month(), start.day(), 0, 0, 0)
    .unwrap()
    .with_timezone(&Utc);
  let end = min(date + Duration::weeks(1), today);
  let end = New_York
    .with_ymd_and_hms(end.year(), end.month(), end.day(), 0, 0, 0)
    .unwrap()
    .with_timezone(&Utc);

  let request = bars::ListReqInit {
    adjustment: Some(bars::Adjustment::All),
    ..Default::default()
  }
  .init(symbol.clone(), start, end, bars::TimeFrame::OneDay);

  let bars = client.issue::<bars::List>(&request);

  let (response1, response2) = join(bars, clock).await;
  let mut bars = response1
    .with_context(|| {
      format!(
        "failed to retrieve historical aggregate bars for {}",
        symbol
      )
    })?
    .bars;
  let clock = response2.context("failed to retrieve current market clock")?;

  let key_fn = |bar: &bars::Bar| bar.time;
  // Alpaca does not document a specific order in which the bars are
  // reported, so sort them to be sure they are ascending.
  bars.sort_unstable_by_key(key_fn);

  let mut utc_date = New_York
    .with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
    .unwrap()
    .with_timezone(&Utc);

  // If the market is currently open (or opens later today) then we are
  // interested in yesterday's date. The reason being that Alpaca
  // would report bars for the ongoing day, and those will change until
  // we reached the end of the trading day.
  if clock.open || clock.next_open.date_naive() == utc_date.date_naive() {
    utc_date = utc_date - Duration::days(1);
  }

  let bar = match bars.binary_search_by_key(&utc_date, key_fn) {
    Ok(index) => bars.get(index).unwrap(),
    Err(index) => {
      // The index reported here is where we would insert. But given
      // that we do not insert we have to subtract one in order to get
      // the previous bar.
      if let Some(bar) = bars.get(index.saturating_sub(1)) {
        bar
      } else {
        // The index does not exist, meaning that we are past the last
        // bar that we received. Just pick the last one then.
        bars
          .last()
          .ok_or_else(|| anyhow!("no historical bars found for {}", symbol))?
      }
    },
  };

  println!(
    "P {date} 23:59:59 {sym} USD {price}",
    date = New_York
      .from_utc_datetime(&bar.time.naive_utc())
      .date_naive(),
    sym = symbol,
    price = bar.close.display().min_precision(2),
  );
  Ok(())
}


/// Retrieve and print the price the given list of assets.
async fn prices_get(client: &Client, symbols: Vec<String>, date: NaiveDate) -> Result<()> {
  // We need the current market clock to decide which price exactly to
  // report. But we only want to make one market clock request. So we
  // have to `Arc` up the error here in order for us to be able to share
  // the future.
  let clock = client.issue::<clock::Get>(&()).map_err(Arc::new).shared();

  #[allow(clippy::manual_try_fold)]
  let () = iter(symbols)
    .map(Ok)
    .map_ok(|symbol| price_get(client, symbol, date, clock.clone()))
    .try_buffer_unordered(32)
    // We use `fold` here to make sure that we process all items, such
    // that all successfully retrieved prices are printed.
    .fold(Ok(()), |acc, result| ready(acc.and(result)))
    .await?;
  Ok(())
}


async fn run() -> Result<()> {
  let args = Args::from_args();
  let level = match args.verbosity {
    0 => LevelFilter::WARN,
    1 => LevelFilter::INFO,
    2 => LevelFilter::DEBUG,
    _ => LevelFilter::TRACE,
  };

  let subscriber = FmtSubscriber::builder()
    .with_writer(stderr)
    .with_max_level(level)
    .with_timer(SystemTime)
    .finish();

  set_global_subscriber(subscriber).with_context(|| "failed to set tracing subscriber")?;

  let api_info =
    ApiInfo::from_env().with_context(|| "failed to retrieve Alpaca environment information")?;
  let mut client = Client::new(api_info);

  match args.command {
    Command::Activity(activity) => {
      let registry = activity.registry;
      let file = File::open(&registry)
        .with_context(|| format!("failed to open registry file {}", registry.display()))?;
      let registry = json_from_reader::<_, HashMap<String, String>>(file)
        .with_context(|| format!("failed to read registry {}", registry.display()))?;

      activities_list(
        &mut client,
        activity.begin,
        activity.force_separate_fees,
        &activity.investment_account,
        &activity.brokerage_account,
        &activity.brokerage_fee_account,
        &activity.dividend_account,
        &activity.sec_fee_account,
        &activity.finra_taf_account,
        &registry,
      )
      .await
    },
    Command::Prices(prices) => prices_get(&client, prices.symbols, prices.date.0).await,
  }
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
        assert_eq!(trade.quantity, Num::from(56));
        assert_eq!(trade.cumulative_quantity, Num::from(56));
        assert!(trade.unfilled_quantity.is_zero());
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
        assert_eq!(trade.quantity, Num::from(175));
        assert_eq!(trade.cumulative_quantity, Num::from(175));
        assert!(trade.unfilled_quantity.is_zero());
      },
      _ => panic!("encountered unexpected account activity"),
    }
  }


  /// Test associating regulatory fees with the corresponding trades.
  #[test]
  fn associate_fees_and_trades() {
    let activities = r#"[
{"id":"11111111111111111::22222222-3333-4444-5555-666666666666","activity_type":"FILL","transaction_time":"2021-06-15T16:17:44.31Z","type":"partial_fill","price":"9.33","qty":"1","side":"sell","symbol":"XYZ","leaves_qty":"55","order_id":"12345678-9012-3456-7890-123456789012","cum_qty":"1","order_status":"partially_filled"},
{"id":"777777777777777777::88888888-9999-1111-2222-333333333333","activity_type":"FILL","transaction_time":"2021-06-15T16:18:56.299Z","type":"partial_fill","price":"9.33","qty":"1","side":"sell","symbol":"XYZ","leaves_qty":"54","order_id":"12345678-9012-3456-7890-123456789012","cum_qty":"2","order_status":"partially_filled"},
{"id":"44444444444444444::55555555-6666-7777-8888-999999999999","activity_type":"FILL","transaction_time":"2021-06-15T16:19:18.136Z","type":"fill","price":"9.33","qty":"54","side":"sell","symbol":"XYZ","leaves_qty":"0","order_id":"12345678-9012-3456-7890-123456789012","cum_qty":"56","order_status":"filled"},
{"id":"11111111111111111::22222222-3333-4444-5555-666666666666","activity_type":"FEE","date":"2021-06-15","net_amount":"-0.01","description":"TAF fee for proceed of 56 shares (3 trades) on 2021-06-15 by 999999999","status":"executed"},
{"id":"77777777777777777::88888888-9999-1111-2222-333333333333","activity_type":"FEE","date":"2021-06-15","net_amount":"-0.01","description":"REG fee for proceed of $522.48 on 2021-06-15 by 999999999","status":"executed"}
]"#;
    let activities = from_json::<VecDeque<account_activities::Activity>>(activities).unwrap();
    let activities = merge_partial_fills(activities);
    let activities = associate_fees_with_trades(activities).unwrap();

    assert_eq!(activities.len(), 1);
    match &activities[0] {
      Activity::Trade(_, fees) => {
        assert_eq!(fees.len(), 2);
        assert_eq!(
          fees[0].description.as_ref().map(String::as_ref),
          Some("TAF fee for proceed of 56 shares (3 trades) on 2021-06-15 by 999999999")
        );
        assert_eq!(
          fees[1].description.as_ref().map(String::as_ref),
          Some("REG fee for proceed of $522.48 on 2021-06-15 by 999999999")
        );
      },
      _ => panic!("encountered unexpected account activity"),
    }
  }
}
