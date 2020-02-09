# Yahoo Ads Api input plugin for Embulk

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration for ReportDefinitionService

- **client_id**: Yahoo client ID. (string, required)
- **client_secret**: Yahoo client secret. (string, required)
- **refresh_token**: Refresh token to get access token.  (string, required)
- **servers**: Servers of API. It is different for YSS and YDN. (string, required)
- **account_id**: Yahoo account id. (string, required)
- **report_type**: YSS report type. It is required only to use YSS. (string, default: `null`)
- **start_date**: Report start date. format:`YYYYMMDD` (string, required)
- **end_date**: Report end date. format:`YYYYMMDD` (string, required)
- **columns**: Report columns. (string, required)
  - **name**: the column name of Yahoo record will be retrieved.
  - **type**: Column values are converted to this embulk type. (Available values options are: boolean, long, double, string, json, timestamp)
  - **format**: Format of the timestamp if type is timestamp. The format for Yahoo is %Y-%m-%dT.

### Example

```yaml
in:
  type: yahoo_ads_api
  client_id: xxxxxx
  client_secret: yyyyyy
  refresh_token: zzzzzz
  servers: https://ads-xxx.yahooapis.jp/api/vX
  account_id: 00000000
  report_type: CAMPAIGN
  start_date: 20191205
  end_date: 20191212
  columns:
    - {name: CAMPAIGN_ID, type: long}
    - {name: CAMPAIGN_NAME, type: string}
    - {name: DAY, type: timestamp, format: "%Y-%m-%d"}
```
## Configuration for StatsService

- **client_id**: Yahoo client ID. (string, required)
- **client_secret**: Yahoo client secret. (string, required)
- **refresh_token**: Refresh token to get access token.  (string, required)
- **servers**: Servers of API. It is different for YSS and YDN. (string, required)
- **stats_type**: The type of information. You can choose from `CAMPAIGN`, `ADGROUP`, `AD`. (string, required)
- **account_id**: Yahoo account id. (string, required)
- **start_date**: Start date. format:`YYYYMMDD` (string, required)
- **end_date**: End date. format:`YYYYMMDD` (string, required)
- **columns**: Report columns. (string, required)
  - **name**: the column name of Yahoo record will be retrieved.
  - **type**: Column values are converted to this embulk type. (Available values options are: boolean, long, double, string, json, timestamp)
  - **format**: Format of the timestamp if type is timestamp. The format for Yahoo is %Y-%m-%dT.

### Example

```yaml
in:
  type: yahoo_ads_api
  client_id: xxxxxx
  client_secret: yyyyyy
  refresh_token: zzzzzz
  servers: https://ads-xxx.yahooapis.jp/api/vX
  account_id: 00000000
  stats_type: CAMPAIGN
  start_date: 20191205
  end_date: 20191212
  columns:
    - {name: CAMPAIGN_ID, type: long}
    - {name: CAMPAIGN_NAME, type: string}
    - {name: DAY, type: timestamp, format: "%Y-%m-%d"}
```


## Build

```
$ rake
```

