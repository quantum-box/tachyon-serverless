# `tsls` CLI リファレンス

`tsls` は gateway の HTTP API（`docs/api.md`）だけを使うクライアント。application / gateway crate には依存しない。

```sh
cargo build -p tachyon-serverless-cli          # target/debug/tsls
export TSLS_API_URL=http://127.0.0.1:8080
export TSLS_TOKEN=dev-token-tenant-a
tsls functions list
```

## 1. グローバルオプション

| オプション | 環境変数 | 既定 | 意味 |
|---|---|---|---|
| `--api-url <url>` | `TSLS_API_URL` | `http://127.0.0.1:8080` | gateway の base URL |
| `--token <token>` | `TSLS_TOKEN` | — | Bearer token。ログ・出力に一切表示しない |
| `--tenant-id <tn_...>` | `TSLS_TENANT_ID` | — | `x-tachyon-tenant-id` ヘッダ（token のテナントと一致必須） |
| `--json` | — | off | サーバの JSON を**そのまま**標準出力へ（表は出さない）。スクリプト用 |
| `--timeout-secs <n>` | — | 120 | HTTP クライアントの per-request timeout |
| `--colon-routes` | — | off | invoke / cancel に `:invoke` `:cancel` 形式を使う（既定は `/invoke` `/cancel`） |

グローバルオプションはサブコマンドの後ろにも書ける（`tsls functions list --json`）。

出力の約束:

- 人向け出力は標準出力にコンパクトな表 / key-value。進捗・注意（`invocation inv_...`、deploy の状態遷移、警告バナー）は標準エラー。
- `--json` では成功時にサーバ本文を無加工で標準出力へ。エラー時もサーバのエラー本文 JSON を標準出力へ出し（`.error.code` を `jq` で読める）、人向けメッセージは標準エラーへ。
- 関数の参照は `fn_...` の id か name のどちらでもよい。name はテナント内の一覧から一致を探す（無ければ exit 2）。

## 2. 終了コード

| exit | 意味 | 例 |
|---|---|---|
| 0 | 成功 | |
| 1 | 使い方 / 設定エラー | 不正なフラグ、token 未設定、読めないファイル、不正な JSON payload |
| 2 | API / 認証 / 検証エラー（invoke の結果ではない 4xx） | `unauthorized` `not_found` `conflict` `invalid_request` `payload_too_large` `capacity_exceeded` `revision_not_ready` `function_deleted`、revision が `failed` |
| 3 | invoke がユーザーコード側で失敗 | `user_error` `crash` `init_error`（`cancelled` もここ） |
| 4 | timeout | `timeout` `queue_timeout`、CLI 側の待機超過（`--wait-timeout`、gateway 起動待ち） |
| 5 | 結果不明 | `outcome_unknown` |
| 6 | platform 側の障害 | `platform_error` `provider_unavailable` `usage_journal_full`（利用量を計測できないので受付拒否、PLT-4642）、本文を解釈できない 5xx、gateway に接続できない |

`functions http` は関数が返した HTTP status がいくつでも exit 0（404 を返す関数は正常）。gateway 側のエラー（本文が API エラー形式）だけ上表に従う。

## 3. コマンド一覧

### functions

| コマンド | 説明 |
|---|---|
| `functions create --name <n> [--description <d>]` | Function を作る |
| `functions list` | 一覧（`--json` は 1 ページ目の本文をそのまま） |
| `functions get <fn>` | 取得 |
| `functions delete <fn>` | 削除（新規と待機中の invoke を止め、実行中は完了を待つ。`functions get` の `deletion` が `deleting` → `deleted`、PLT-4635） |
| `functions deploy ...` | artifact upload → revision 作成 → ready まで poll → alias `prod` 表示（§4） |
| `functions invoke <fn> ...` | 同期 invoke（§5） |
| `functions http <fn> ...` | HTTP アダプタ経由でリクエスト（§6） |
| `functions invocations <fn> [--limit 20]` | 履歴の表 |
| `functions invocation <inv_id>` | 詳細（attempts, timings, boot_evidence） |
| `functions logs --invocation <inv_id>` / `--function <fn> [--limit 5]` | ログ（§7） |
| `functions revisions <fn>` / `functions revision <fn> <rev_id>` | revision 一覧 / 詳細（spec を含む） |
| `functions aliases <fn>` | alias 一覧 |
| `functions alias-set <fn> --alias prod --revision-id <rev> [--expected-generation <g>]` | alias を（CAS で）更新 |
| `functions rollback <fn> [--alias prod] [--to <rev_id>]` | 直前の revision に戻す（§8） |
| `functions cancel <inv_id>` | 実行中の invocation を cancel |

### triggers（PLT-4641）

| コマンド | 説明 |
|---|---|
| `triggers create <fn> --name <n> --kind cron --schedule '<expr>' [--timezone Asia/Tokyo] [--payload '<json>'] [--missed-run skip\|run-once\|run-all --max-runs N] [--alias prod \| --revision-id <rev>] [--disabled]` | cron trigger を作る（`POST /v1/functions/{id}/triggers`）。式は 5 field か、先頭に秒を足した 6 field（シェルの展開を避けるため quote する） |
| `triggers create <fn> --name <n> --kind webhook [--tolerance-seconds 300] [--max-body-bytes N] [--event-id-header <h>]` | webhook trigger を作る。**secret は標準出力にこの 1 回だけ表示される**（`secret whsec_...`、`--json` なら `.secret`）。URL は `webhook.url` |
| `triggers list <fn>` / `triggers get <fn> <trg_id>` | 一覧 / 詳細（secret は表示しない。`secret_fingerprint` だけ） |
| `triggers update <fn> <trg_id> [--enable \| --disable] [--schedule ...] [--timezone ...] [--payload ...] [--missed-run ...] [--tolerance-seconds ...] [--rotate-secret] [--expected-generation <g>]` | `PATCH`。`--rotate-secret` は新しい secret を 1 回だけ表示する。generation 不一致は 409 → exit 2 |
| `triggers delete <fn> <trg_id>` | 削除。以後の fire は無い。受付済みの invocation は続く |
| `triggers fires <fn> <trg_id> [--limit 50]` | fire の記録（`cron:<予定時刻>` / `event:<event id>`、`accepted` / `refused`、invocation id） |
| `triggers webhook-sign (--secret-env VAR \| --secret whsec_...) [--body '<raw>' \| --body-file <path>] [--timestamp <unix>]` | **送信しない**。`x-tachyon-webhook-timestamp` と `x-tachyon-webhook-signature` の header 行を表示する（`--json` なら `{timestamp, signature, headers}`）。`--secret` は process 一覧に見えるので `--secret-env` を推奨。token は不要 |

webhook の試験例:

```sh
SECRET=$(tsls --json triggers create hello --name orders --kind webhook | jq -r .secret)
BODY='{"order":42}'
tsls triggers webhook-sign --secret-env SECRET --body "$BODY"   # 2 行の header
TS=$(date +%s); SIG=$(printf '%s.%s' "$TS" "$BODY" | openssl dgst -sha256 -hmac "$SECRET" | sed 's/^.*= *//')
curl -X POST "$TSLS_API_URL/v1/hooks/<trg_id>" -H "x-tachyon-webhook-timestamp: $TS" \
  -H "x-tachyon-webhook-signature: v1=$SIG" -H 'x-tachyon-webhook-id: evt-1' --data-binary "$BODY"
```

### その他

| コマンド | 説明 |
|---|---|
| `provider` | `GET /v1/provider`。kind / isolation / dev_only と capability 表、preflight を表示。`dev_only` なら「隔離なし」の警告を標準エラーに出す |
| `capacity` | `GET /v1/capacity`（PLT-4634）。node（region、hosts、host scale-out の可否、容量、環境ごとの overhead）、予約の合計、状態別の環境数、in-flight、待ち行列、start rate、scaling（reconcile 間隔・既定 TTL / cooldown・drain timeout・warm pool の有無・「環境 0 は host 費用 0 ではない」）と、自 tenant の revision ごとの route 状態 / desired / min_ready / starting / busy / idle / draining / queued / 到着率 / breaker / 最後の scale event（PLT-4635）を表示。`--json` で本文そのまま |
| `health` | `GET /healthz` と `GET /readyz`。どちらかが 2xx でなければ exit 6 |
| `usage [--from <RFC3339 or YYYY-MM-DD>] [--to ...] [--group-by function\|day\|function,day\|none] [--function <fn>]` | `GET /v1/usage`（PLT-4642）。token の tenant の**仮**利用量・仮料金。1 行目は常に `PROVISIONAL - provisional usage estimate: not an invoice, ...`。続いて tenant・範囲・価格表（version、effective_from、通貨、課金区間）・collector が運んだ時刻・journal に入らなかった event 数、行ごと（と `TOTAL`）の invocations / attempts / retries / timeouts / handler ms / billable ms / unmetered / 仮料金（通貨単位の小数）。`--json` で本文そのまま（原価・guest 申告・丸め規則を含む） |
| `dev --binary <path> ...` | 使い捨て gateway で 1 バイナリを end-to-end 実行（§9） |

## 4. `functions deploy`

```
tsls functions deploy --function <name|id> --binary <path>
    [--arch auto|x86_64|aarch64]          # auto = CLI を動かしているホストの arch
    [--memory-mib 256] [--cpu-millis 500] [--ephemeral-storage-mib 256]  # /tmp の上限（32..=2048）
    [--timeout-seconds 30] [--init-timeout-seconds 30] [--max-concurrency 4]
    [--min-ready 0] [--idle-ttl-seconds <s>] [--scale-down-cooldown-seconds <s>]  # PLT-4635
    [--env KEY=VALUE]... [--secret ENV_NAME=binding_ref]...
    [--egress none|restricted|public-web]  # 既定 none（NIC なし）
    [--egress-allow [tcp|udp:]CIDR:PORT[,PORT...]]...  # restricted の許可先（必須、他 profile では不可）
    [--description <text>]
    [--region <region>]                    # 例 jp。node の region が違えば invoke は 503 placement
    [--no-publish]                         # alias prod を動かさない
    [--wait | --no-wait] [--wait-timeout 120]
```

流れ: `POST /v1/artifacts`（octet-stream）→ `POST /v1/functions/{id}/revisions` → 250 ms 間隔で `GET .../revisions/{rev}` を `ready` | `failed` まで poll（最大 `--wait-timeout` 秒）→ `--no-publish` でなければ `GET .../aliases/prod` を表示。

- `--json` は最終的な `RevisionResponse` を出力する（`jq -r .id` で revision id が取れる）。
- revision が `failed` → exit 2（理由を表示）。`--wait-timeout` 超過 → exit 4。
- `--secret` の値は CLI を通らない。`binding_ref` は gateway 設定 `[[secrets.bindings]]` で解決される。
- `--egress restricted` は `--egress-allow` で許可先を 1 つ以上指定する（例 `--egress-allow 1.1.1.1/32:443`、`--egress-allow udp:1.1.1.1/32:53`）。IPv4 CIDR のみで、private・link-local・metadata・loopback などの範囲は 400 になる。firecracker provider で `restricted` / `public-web` を実行するには host 側の権限が要る（`docs/adr/0005-egress-profiles.md`）。
- `--min-ready N` は alias が route している間に用意しておく環境数（既定 0 = 無負荷なら環境 0）。gateway の環境再利用（`[pool] enabled` と provider の idle capability）が無いと満たされない（`tsls capacity` の `warm pool off`）。先行起動した環境は invocation が無くても node の容量を使う。`--idle-ttl-seconds` / `--scale-down-cooldown-seconds` は省略すると gateway の既定（`[pool] idle_ttl_seconds` / `[scaling] scale_down_cooldown_seconds`）。`docs/adr/0009-scale-to-zero-and-drain.md`。
- `--region jp` は revision の `required_region`。gateway の `[capacity.node] region` が一致しない node では、容量に関係なく invoke が 503（`error.reason = placement`、exit 6）になる（`docs/adr/0006-autoscaling-and-admission.md`）。
- firecracker provider では `--binary` は static Linux (musl) バイナリでなければならない。CLI は形式を検証しない（provider の validate で `failed` になる）。

## 5. `functions invoke`

```
tsls functions invoke <fn> [--payload '<json>' | --payload-file <path>]
    [--alias prod | --revision-id <rev>]
    [--client-timeout-ms <n>] [--idempotency-key <k>]
```

- payload 省略時は `{}`。JSON として不正なら exit 1。
- 成功: 標準出力に handler の出力（人向けは整形、`--json` は無加工）。標準エラーに `invocation inv_... trace=...`。
- 失敗: exit は §2 の対応表。標準エラーに `error: user_error (HTTP 502): ... [type=Handler.Error] [invocation=inv_...]`、`--json` なら標準出力にエラー本文。
- 隔離なしの警告: 応答を受け取った後に認証付きで `GET /v1/provider` を引き、`dev_only` なら成功・失敗を問わず標準エラーに `provider` と同じ「隔離なし」の警告を出す（ADR-0002 決定 4）。引けなければ標準エラーに `warning: could not determine provider isolation (...)` を出すだけで、stdout と exit code は変えない。

## 6. `functions http`

```
tsls functions http <fn> [--method GET] [--path /] [--data <body> | --data-file <path>]
    [--header 'Name: value']... [-v|--verbose]
```

`/v1/functions/{id}/http<path>` に対してリクエストし、関数の応答をそのまま表示する。人向け出力は `HTTP <status>` 行、`-v` ならヘッダ、空行、本文。`--json` は `{"status": 404, "headers": [["name","value"],...], "body": "..."}`（本文は UTF-8 として表示）。

§5 と同じく、応答を受け取った後に `GET /v1/provider` を引き、`dev_only` なら（gateway 側エラーを含む）どの結果でも標準エラーに「隔離なし」の警告を出す。引けなければ警告を出すだけで、stdout と exit code は変えない。`tsls dev` は起動時のバナーで代える（§9）。

## 7. `functions logs`

```
tsls functions logs --invocation <inv_id>
tsls functions logs --function <fn> [--limit 5]     # 直近 N 件の invocation のログを古い順に
```

1 行の形式は `2026-09-15T01:10:00.300Z [stdout/handler] line`。行長上限で切れた行には ` [truncated]`、保持上限で捨てられた行があれば末尾に `-- dropped: ...` を出す。`--json` は `LogsResponse` を無加工で（`--function` の時は invocation ごとに 1 行の NDJSON）。

## 8. `functions rollback`

`GET /v1/functions/{id}/aliases/{alias}` で現在の `revision_id` / `generation` / `previous_revision_id` を読み、`--to` が無ければ `previous_revision_id` へ、`expected_generation = 現在の generation` を付けて `PUT` する。並行更新があれば 409 `conflict` → exit 2。出力は `from` / `to` / `generation a -> b`。`previous_revision_id` が無ければ exit 2（`--to` を使う）。

## 9. `tsls dev`

```
tsls dev --binary <path> [--payload '{}'] [--gateway-binary <path>] [--bridge-binary <path>]
    [--gateway-config-flag --config] [--timeout-seconds 30] [--keep]
```

1. 一時ディレクトリに gateway 設定を生成する（`profile = "dev"`、`[provider] kind = "process"`、`bridge_binary`、`workdir`、`data_dir`、`listen = 127.0.0.1:<空きポート>`、ランダムな dev token 1 つ）。`docs/architecture.md` §4 の形式。
2. `tachyon-serverless-gateway`（`tsls` と同じディレクトリから自動検出、または `--gateway-binary`）を `--config <path>` で起動し（環境変数 `TACHYON_GATEWAY_CONFIG` にも同じパスを入れる）、`/healthz` を最大 10 秒待つ。
3. Function `dev` を作り、`--binary` を deploy、`--payload` で 1 回 invoke、出力とログを表示する。
4. gateway に SIGTERM を送って終了を待ち（5 秒で SIGKILL）、`--keep` でなければ一時ディレクトリを削除する。

**process provider は隔離を提供しない。** 関数はこのユーザーの子プロセスとしてそのまま動く。開発専用であり、成功しても microVM での成功を意味しない。起動時にその旨のバナーを標準エラーに出す。exit code は invoke の結果に従う。

## 10. 例

```sh
# 関数を作って deploy
tsls functions create --name hello
tsls functions deploy --function hello --binary target/debug/example-hello \
  --env GREETING=v1 --secret DEMO_SECRET=demo-secret

# invoke
tsls functions invoke hello --payload '{"name":"demo"}'
tsls functions invoke hello --payload '{"fail":true}'; echo "exit=$?"     # 3

# 履歴・ログ・証跡
tsls functions invocations hello
tsls functions invocation inv_01j7z2k3m4n5p6q7r8s9t0v1w2
tsls functions logs --invocation inv_01j7z2k3m4n5p6q7r8s9t0v1w2

# rollback
tsls functions deploy --function hello --binary target/debug/example-hello --env GREETING=v2
tsls functions rollback hello

# HTTP
tsls functions http http-axum --method POST --path /echo --data 'hi' -v

# provider / health
tsls provider
tsls health
```
