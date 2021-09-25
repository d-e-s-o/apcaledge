Unreleased
----------
- Added support for merging partial fills for same order and at same
  price to reduce number of generated entries
- Added support for reporting regulatory fees
  - Added logic for associating regulatory fees with the trades they
    belong to
  - Introduced `--force-separate-fees` option to opt out of this
    association logic
- Added support for acquisition non-trade activities
- Made dividend and brokerage fee account names to use configurable
- Bumped `apca` dependency to `0.19`


0.2.0
-----
- Added support paging through all account activities
  - Introduced `--begin` option to control which date to start reporting
    at
- Print more recent activity data at the bottom
- Bumped `apca` dependency to `0.18`
- Bumped `tokio` dependency to `1.0`


0.1.2
-----
- Added support for emitting entries for the following non-trade
  activities:
  - Dividends
  - Pass-through charges
- Made investment and brokerage account names configurable through
  program options
- Bumped `apca` dependency to `0.16`


0.1.1
-----
- Changed date format used from `%Y/%m/%d` to `%Y-%m-%d`
- Bumped `apca` dependency to `0.15`
- Bumped `tracing-subscriber` dependency to `0.2`


0.1.0
-----
- Initial release
