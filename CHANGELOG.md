Unreleased
----------
- Made dividend account name to use configurable
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
