# Yahoo Ads Api input plugin for Embulk

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **client_id**: Yahoo社から提供されるクライアントID (string, required)
- **client_secret**: Yahoo社から提供されるクライアントシークレット (string, required)
- **refresh_token**: アクセストークンを再取得するためのリフレッシュトークン (string, required)
- **servers**: APIのサーバー。YDNとYSSで異なる。 (string, required)
- **account_id**: 取得するアカウントのID (string, required)
- **report_type**: レポートタイプ。YSSを利用する時のみ必須。 (string, default: `null`)
- **date_range_min**: 取得するレポート期間の開始日. フォーマットは"YYYYMMDD" (string, required)
- **date_range_max**: 取得するレポート期間の終了日. フォーマットは"YYYYMMDD" (string, required)
- **columns**: 取得するレポートの列 (string, required)

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

