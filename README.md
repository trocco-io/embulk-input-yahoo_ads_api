# Yahoo Ads Api input plugin for Embulk

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **client_id**: Yahoo client ID. (string, required)
- **client_secret**: Yahoo client secret. (string, required)
- **refresh_token**: Refresh token to get access token.  (string, required)
- **servers**: Servers of API. It is different for YSS and YDN. (string, required)
- **account_id**: Yahoo account id. (string, required)
- **report_type**: YSS Report type. It is required only to use YSS. (string, default: `null`)
- **date_range_min**: Report start date. format:`YYYYMMDD` (string, required)
- **date_range_max**: Report end date. format:`YYYYMMDD` (string, required)
- **columns**: Report columns. (string, required)

## Example

```yaml
in:
  type: yahoo_ads_api
  client_id: xxxxxx
  client_secret: yyyyyy
  refresh_token: zzzzzz
  servers: https://ads-xxx.yahooapis.jp/api/vX
  account_id: 00000000
  report_type: CAMPAIGN
  date_range_min: 20191205
  date_range_max: 20191212
  columns:
    - COST
    - IMPS
```


## Build

```
$ rake
```

