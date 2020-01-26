// Copyright (C) 2020 Daniel Mueller <deso@posteo.net>
// SPDX-License-Identifier: GPL-3.0-or-later

use std::convert::TryInto;
use std::io::stdout;
use std::io::Write;
use std::process::exit;
use std::time::SystemTime;
use std::time::SystemTimeError;
use std::time::UNIX_EPOCH;

use apca::api::v2::account;
use apca::api::v2::account_activities;
use apca::ApiInfo;
use apca::Client;

use anyhow::Context;
use anyhow::Error;

use chrono::offset::TimeZone;
use chrono::offset::Utc;
use chrono::DateTime;

use num_decimal::Num;

use tokio::runtime::Runtime;

const FROM_ACCOUNT: &str = "Assets:Investments:Stock";
const TO_ACCOUNT: &str = "Assets:Alpaca Brokerage";

/// Format a price value.
fn format_price(price: &Num, currency: &str) -> String {
  format!("{:.2} {}", price, currency)
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
    .map(|time| time.date().format("%Y/%m/%d").to_string().into())
    .unwrap()
}

fn print_trade(trade: &account_activities::TradeActivity, currency: &str) {
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
    name = trade.symbol,
    from = FROM_ACCOUNT,
    to = TO_ACCOUNT,
    qty = trade.quantity as i32 * multiplier,
    sym = trade.symbol,
    price = format_price(&trade.price, &currency),
    total = format_price(
      &(&trade.price * trade.quantity as i32 * -multiplier),
      &currency
    ),
  );
}

async fn activities_list(client: &mut Client) -> Result<(), Error> {
  let request = account_activities::ActivityReq {
    types: Some(vec![account_activities::ActivityType::Fill]),
  };
  let currency = client
    .issue::<account::Get>(())
    .await
    .with_context(|| "failed to retrieve account information")?
    .currency;
  let activities = client
    .issue::<account_activities::Get>(request)
    .await
    .with_context(|| "failed to retrieve account activities")?;

  for activity in activities {
    match activity {
      account_activities::Activity::Trade(trade) => print_trade(&trade, &currency),
      account_activities::Activity::NonTrade(..) => (),
    }
  }
  Ok(())
}

async fn run() -> Result<(), Error> {
  let api_info =
    ApiInfo::from_env().with_context(|| "failed to retrieve Alpaca environment information")?;
  let mut client = Client::new(api_info);

  activities_list(&mut client).await
}

fn main() {
  let mut rt = Runtime::new().unwrap();
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
