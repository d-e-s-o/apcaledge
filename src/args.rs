// Copyright (C) 2022-2023 Daniel Mueller <deso@posteo.net>
// SPDX-License-Identifier: GPL-3.0-or-later

use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::path::PathBuf;
use std::str::FromStr;

use chrono::Local;
use chrono::NaiveDate;

use structopt::StructOpt;


const DEFAULT_INVESTMENT_ACCOUNT: &str = "Assets:Investments:Alpaca:Stock";
const DEFAULT_BROKERAGE_ACCOUNT: &str = "Assets:Alpaca Brokerage";
const DEFAULT_BROKERAGE_FEE_ACCOUNT: &str = "Expenses:Broker:Fee";
const DEFAULT_DIVIDEND_ACCOUNT: &str = "Income:Dividend";
const DEFAULT_SEC_FEE_ACCOUNT: &str = "Expenses:Broker:SEC Fee";
const DEFAULT_FINRA_TAF_ACCOUNT: &str = "Expenses:Broker:FINRA TAF";


/// A command line client for formatting Alpaca trades in Ledger format.
#[derive(Debug, StructOpt)]
#[structopt(about)]
pub struct Args {
  #[structopt(subcommand)]
  pub command: Command,
  /// Increase verbosity (can be supplied multiple times).
  #[structopt(short = "v", long = "verbose", global = true, parse(from_occurrences))]
  pub verbosity: usize,
}


#[derive(Debug, StructOpt)]
pub enum Command {
  /// List trades and other account activity.
  Activity(Activity),
  /// Import trades and other account activity.
  Prices(Prices),
}


/// Retrieve and print account activity.
#[derive(Debug, StructOpt)]
pub struct Activity {
  /// The path to the JSON registry for looking up names from symbols.
  pub registry: PathBuf,
  /// Only show activities dated at the given date or after (format:
  /// yyyy-mm-dd).
  #[structopt(short, long)]
  pub begin: Option<NaiveDate>,
  /// Force keeping regulatory fees separate and not match them up with
  /// trades on a best-effort basis.
  #[structopt(long)]
  pub force_separate_fees: bool,
  /// The name of the investment account, i.e., the one holding the
  /// shares.
  #[structopt(long, default_value = DEFAULT_INVESTMENT_ACCOUNT)]
  pub investment_account: String,
  /// The name of the brokerage account, i.e., the one holding any
  /// uninvested cash.
  #[structopt(long, default_value = DEFAULT_BROKERAGE_ACCOUNT)]
  pub brokerage_account: String,
  /// The name of the brokerage's fee account.
  #[structopt(long, default_value = DEFAULT_BROKERAGE_FEE_ACCOUNT)]
  pub brokerage_fee_account: String,
  /// The name of the account to account dividend payments against.
  #[structopt(long, default_value = DEFAULT_DIVIDEND_ACCOUNT)]
  pub dividend_account: String,
  /// The name of the account to use for regulatory fees by the SEC.
  #[structopt(long, default_value = DEFAULT_SEC_FEE_ACCOUNT)]
  pub sec_fee_account: String,
  /// The name of the account to use for FINRA trade activity fees.
  #[structopt(long, default_value = DEFAULT_FINRA_TAF_ACCOUNT)]
  pub finra_taf_account: String,
}


/// A structopt usable date type that defaults to "today".
#[derive(Debug)]
pub struct Date(pub NaiveDate);

impl Default for Date {
  fn default() -> Self {
    Self(Local::now().date_naive())
  }
}

impl Display for Date {
  fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
    Display::fmt(&self.0, f)
  }
}

impl FromStr for Date {
  type Err = <NaiveDate as FromStr>::Err;

  fn from_str(string: &str) -> Result<Self, Self::Err> {
    NaiveDate::from_str(string).map(Self)
  }
}


/// Retrieve the historic prices for a set of assets.
#[derive(Debug, StructOpt)]
pub struct Prices {
  /// The symbols for which to retrieve the most recent price.
  pub symbols: Vec<String>,
  /// The date for which to retrieve the price or, if not specified,
  /// defaults today's date (format: yyyy-mm-dd).
  #[structopt(short, long, default_value)]
  pub date: Date,
}
