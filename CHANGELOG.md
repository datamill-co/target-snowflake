# Changelog

## 0.2.4

- **NOTE:** We are bumping this a _couple_ major versions to allow this to stay in sync with
  [Target-Postgres](https://github.com/datamill-co/target-postgres). Naturally this comes with
  a number of enhancements and we encourage you to checkout the CHANGELOG there to get up to speed.
  We do not expect any issues for folks, but there is a data change under the hood to be aware of.
- **FIX:** Missed quoting of CSV was leading to issues around more columns seen in text fields with commas
  - [LINK](https://github.com/datamill-co/target-snowflake/commit/62986b6f9f935b27bba6e5ec62d0da7b464a29a9)

## 0.0.1

- **BETA:** Target Snowflake is still in beta, though we are encouraging people to
  use it and submit issues to our Github. Thanks in advance for the patience and help!
- **FEATURES:**
  - [Parity with target-postgres v0.1.10](https://github.com/datamill-co/target-postgres/tree/v0.1.10)
