# ADR-0014: cron と署名付き webhook の trigger は、fire 行を invokeAsync の受付トランザクションに入れた「非同期 invocation の受付」として実装する（PLT-4641）

## ステータス

Accepted（2026-09-17、PLT-4641）。範囲は最小の cron trigger と、検証用 source（`generic-hmac`）の署名付き webhook。汎用の外部 SaaS connector（GitHub・Stripe 等の source 別検証）は非対象。

実装:

- model・trait: `crates/application/src/repository/triggers.rs`（`Trigger`、`FireRecord`、`TriggerRepository`）
- 台帳: `crates/application/src/repository/sqlite/triggers.rs`、migration `007_triggers.sql`（`triggers`、`trigger_fires`、`trigger_scheduler`）
- 受付の共有: `crates/application/src/repository/sqlite/outbox.rs::accept_in`（`accept_async` と fire の両方が使う行の書き込み）、`crates/application/src/services/invoke_async/mod.rs::accept_for_trigger`
- service: `crates/application/src/services/triggers/mod.rs`（CRUD、scheduler、webhook）、`cron.rs`（式と time zone）、`webhook.rs`（署名）、`config.rs`（`[triggers]`）
- 認可: `crates/application/src/control/cache.rs::authorize_tenant`
- API: `apps/gateway/src/trigger_handlers.rs`、scheduler の loop は `apps/gateway/src/lib.rs::serve`
- CLI: `apps/cli/src/commands/triggers.rs`（`tsls triggers create|list|get|update|delete|fires|webhook-sign`）
- E2E: `scripts/queue/triggers-e2e.sh`、証跡 `docs/evidence/triggers-e2e-*/`

## コンテキスト

- HTTP の手動呼出（同期 invoke、`invokeAsync`）以外に、時刻と外部イベントから関数を動かしたい。
- trigger が独自に「実行」や「retry」を持つと、quota・認可・Revision 固定・再試行・DLQ が 2 系統になる。PLT-4639 で受付（台帳の 1 トランザクション + outbox）、PLT-4640 で dispatcher・retry・DLQ を作っているので、trigger はその入口の 1 つであるべき。
- 受入条件は「同じ予定時刻を controller 再起動で重複作成しない」「invalid 署名・期限切れ・過大 body を永続受付前に拒否する」「disable / delete 後の新規発火を止め、受付済みの扱いを説明できる」「trigger からも quota / 認可 / Revision 固定と共通の retry 経路を使う」。

### 既存 CronJob（quantum-box/tachyon-apps）の調査

「既存 CronJob を調査・再利用」の指示に対し、tachyon-apps を読み取りだけで調べた（2026-09-17、`gh` の code search と contents API）。

| 観点 | tachyon-apps の実装 | 参照 |
|---|---|---|
| 位置 | Cloud Apps の Cron Jobs MVP（PLT-685）。DB 記録と API / UI だけで、式を解釈して時刻を計算し実行する scheduler は無い | `packages/compute/domain/src/cron_job.rs`、`packages/compute/src/usecase/{register_cron_jobs,trigger_cron_job,list_cron_jobs,list_cron_job_runs}.rs`、`packages/compute/src/adapter/axum/cron_job_handler.rs`、`docs/src/tasks/completed/plt-685-cron-jobs/task.md` |
| 式の library | cron crate を使っていない（`cron` / `croner` / `tokio-cron-scheduler` / `saffron` のどれも Cargo.toml に無い）。`schedule` は検証なしの文字列で、5 field と文書化されている。解釈は Cloudflare Workers Cron Triggers / AWS EventBridge に任せる計画だったが provider の実装は無い | `cron_job.rs` L204、migration `20260420020014_create_cron_jobs.up.sql` L5 |
| time zone | UTC だけ（`DateTime<Utc>`、job ごとの zone 無し、chrono-tz 無し）。DST は扱わない | 同上 |
| missed run | 無い。`next_run_at` / `last_run_at` の列はあるが計算・更新されない | `CronJob::create` |
| 一意性 | 定義の `UNIQUE (app_id, name)` だけ。`cron_job_runs` に (job, 予定時刻) の一意キーは無く、lock・leader election も無い | migration L14 |
| 状態・API | `Active` / `Inactive`。`POST /v1/apps/{app_id}/cron` が app の job 集合を丸ごと置き換える（upsert + 欠けた job の削除）。手動 trigger は `CronJobRun{running, manual}` の行を足すだけで何も呼ばない。retry なし | `cron_job_handler.rs`、`trigger_cron_job.rs` L78-79 |

結論: 再利用できる parser・time zone 処理・catch-up・重複防止の設計は無い（このリポジトリは tachyon-apps の crate にも依存しない）。借りたのは次だけ。

- **5 field の式を基本にする**（tachyon-apps と同じ）。seconds 付きの 6 field は、検証環境で数秒間隔の発火を観測するための拡張として足した。
- **有効 / 無効の 2 状態**（`Active` / `Inactive` → `enabled` / `disabled`）と、定義とは別の **実行記録の行**（`cron_job_runs` → `trigger_fires`）。ただし実行記録に (trigger, 予定時刻) の一意キーを持たせ、それを重複防止の根拠にした（tachyon-apps に無い点）。
- ID は tachyon と同じ `<prefix>_<ULID>`（`trg_`）。

library は外部 crate を使わず、式の解釈と time zone の写像を自前で持つ（§2）。IANA zone の表は `chrono-tz`（`chrono` の公式な zone database crate）を使う。cron crate を採らなかった理由: 受入条件が「存在しない local 時刻は skip、2 回ある local 時刻は 1 回」という決定的な意味を一意キーに結び付けることを要求し、既存 crate は DST の意味が crate ごとに異なる（OCPS 準拠の crate は gap の時刻を後ろにずらして実行する、別の crate は曖昧時刻を 2 回返す版がある）。式の解釈は 200 行程度で、意味を test で固定する方が安全と判断した。

## 選択肢

| 案 | 内容 | 採否 |
|---|---|---|
| A | fire ＝ `invokeAsync` の受付。fire 行（一意キー）を受付と同じトランザクションに入れ、同じトランザクションで trigger の状態も読む | **採用** |
| B | scheduler が fire 行を先に commit し、別に invokeAsync を呼ぶ | 不採用。fire 行 commit 後・受付前に落ちると「発火済みだが invocation が無い」時刻が残り、それを拾う仕組み（= outbox）が要る。disable との競合も 2 トランザクションにまたがる |
| C | Idempotency-Key だけで重複を防ぐ（fire 行を持たない） | 不採用。key は invocation が terminal になると `idempotency_retention_seconds` で失効し、再起動後に同じ時刻を再計算したときの防御にならない。refused の記録や webhook の署名単位の dedup も表せない |
| D | trigger 専用の実行・retry キュー | 不採用。quota・認可・Revision 固定・retry・DLQ が 2 系統になる |

## 決定

### 1. 共通の受付経路

- すべての fire は `AsyncInvokeService::accept_for_trigger` を通る。中身は `accept` と同じで、違いは台帳のトランザクション（`TriggerRepository::accept_trigger_fire`）だけ:
  1. trigger 行を読み直す。削除済み・`enabled` でない・（cron は）scheduler が読んだ generation と違う → `Inactive`（何も書かない）
  2. `(trigger_id, fire_key)` の fire 行がある、または（webhook は）同じ署名 digest の行がある → `AlreadyFired`（何も書かない）
  3. `accept_in`: Idempotency-Key、backlog の上限、invocation、key、入力、outbox event（ADR-0010 §1.6 と同じ行）
  4. fire 行（`outcome = accepted`、invocation id）。主キー違反は全体を rollback
- 認可: trigger には bearer token が無い。fire の principal は trigger の tenant + `invoke` role（subject `trigger:<id>`）で、**fire のたびに** 設定 cache で tenant が配信済み・auth lease 内であることを確かめる（`ConfigCache::authorize_tenant`、`authenticate` の tenant 検査と同じ）。function / alias / revision / policy の解決は `resolve` そのもの。trigger を作れるのは `deploy` role（management API）。
- Revision は受付時に固定する（target が alias なら fire の時点の alias、`revision_id` ならその revision）。
- quota: 受付の上限（outbox の件数・滞留時間、ADR-0010 §4）は同じ。実行時の quota・admission・retry・DLQ は dispatcher（PLT-4640）が invocation に対して行い、trigger は関知しない。
- 利用量の計測（PLT-4642、ADR-0012）: trigger の fire は **受付時には計測しない**。`invokeAsync` の受付自体が usage journal の admission（`UsageMeter::admit`）を呼ばないのと同じで、trigger 由来の invocation は **実行時に**、dispatcher（PLT-4640）が attempt を始めるときの admission と `AttemptSettled` で他の非同期 invocation と同じく計測される。journal が満杯・停止なら実行の開始が fail closed で止まり、受付済みの fire は `queued` のまま待つ。trigger 固有の usage event は無い（fire の記録は `trigger_fires`、課金の根拠にはしない）。
- 冪等キー: cron `cron:{trigger_id}:{scheduled_at_utc}`（`%Y-%m-%dT%H:%M:%SZ`）、webhook `webhook:{trigger_id}:{event_id}`。fire 行と同じトランザクションで結ばれる。

### 2. cron

**式**（`services/triggers/cron.rs`）: 5 field `minute hour day-of-month month day-of-week`（秒 = 0）、または先頭に秒を足した 6 field。`*`、`n`、`a-b`、`*/s`、`a-b/s`、`a/s`、`,` の列挙、`JAN`-`DEC`、`SUN`-`SAT`（0 と 7 は日曜）、`@yearly` / `@annually` / `@monthly` / `@weekly` / `@daily` / `@midnight` / `@hourly`。day-of-month と day-of-week の両方を制限した場合は **どちらか** が合えば一致（Vixie cron）。5 年以内に一致しない式（`0 0 30 2 *`）は作成時に 400。

**time zone**: trigger ごとの IANA 名（既定 `UTC`）。式は zone の **wall-clock 時刻** に対して評価し、instant への写像は:

| local 時刻 | 扱い | 例（`America/New_York`） |
|---|---|---|
| 1 つの instant に対応 | その instant | — |
| 存在しない（DST 開始で飛ぶ時間） | **skip**（その日は発火しない） | `30 2 * * *` は 2026-03-08 に発火しない |
| 2 回ある（DST 終了で繰り返す時間） | **早い方の instant で 1 回** | `30 1 * * *` は 2026-11-01 に 05:30Z の 1 回、`*/15 * * * *` はその 1 時間に 4 回（8 回ではない） |

「時刻を後ろにずらして実行する」「両方の instant で実行する」は採らない。どちらも予定時刻を 1 つの一意キーに決められない。

**cursor**: 有効な cron trigger は `next_fire_at`（未処理の次の予定時刻、UTC）を持つ。作成・有効化・式 / zone の変更のときに **現在時刻の次** から始める（無効だった期間や前の式の過去は missed run ではない）。

**scheduler の pass**（`TriggerService::run_scheduler_once`、gateway が `scheduler_interval_ms` と最早の `next_fire_at` の短い方ごとに回す）:

1. dispatcher が fenced なら何もしない。scheduler lease（`trigger_scheduler` の 1 行、owner = dispatcher id、TTL = `[dispatcher] lease_ttl_seconds`）を取るか更新する。他の owner の lease が有効なら何もしない。期限切れ、または owner の dispatcher が停止・reclaim 済み（PLT-4631 の `dispatchers` 行）なら奪う。graceful shutdown では手放す。
2. `next_fire_at <= now` の有効な cron trigger（最大 `scheduler_batch` 件）について、`[max(cursor, now - max_catchup_seconds), now]` の予定時刻を列挙する。
3. `now - grace_seconds` 以降の時刻は **on time**、それより前は **late**。missed-run policy:

   | policy | late の時刻 | on time |
   |---|---|---|
   | `skip`（既定） | 実行しない（件数を log） | 実行 |
   | `run_once` | 最新の 1 つだけ実行 | 実行 |
   | `run_all { max_runs }` | 新しい方から `max_runs` 個（上限 `[triggers] max_run_all`）を古い順に実行 | 実行 |

   `max_catchup_seconds`（既定 24 時間）より古い時刻は policy に関係なく実行しない（停止が長いときに列挙も実行も有界にする）。
4. 各時刻を §1 の経路で fire する。結果ごとに:
   - accepted / already fired → 次の時刻へ
   - `Inactive`（無効化・削除・変更の競合）→ この trigger の pass を終える（cursor は動かさない。変更側が cursor を決め直している）
   - 一時的な拒否（`backlog` / `queue_*`、設定 cache の期限切れ・未配信、store 障害）→ cursor をこの時刻に置いて pass を終える（次の pass で再試行。そのときに late になれば policy に従う）。fire 行は書かない
   - 恒久的な拒否（関数削除、alias / revision 無し、revision 未 ready、policy 拒否、未知 tenant、入力過大）→ `outcome = refused` と理由の fire 行を書き、次の時刻へ。関数削除なら trigger を `disabled`（`status_reason = function_deleted`）にする
5. cursor を `now` の次の予定時刻へ CAS（generation が同じで enabled の場合だけ、後戻りしない）。

**再起動で重複しない理由**: 予定時刻の fire 行は invocation と同じトランザクションで commit され、主キー `(trigger_id, "cron:" + scheduled_at)` を持つ。fire の commit 後・cursor の移動前に落ちても、再起動後の pass は同じ時刻を列挙し、fire 行を見つけて何も作らない。2 つの scheduler が同時に同じ時刻を処理しても（lease の引き継ぎ、時計のずれ）、主キーで 1 つだけが commit する。lease は無駄な仕事を減らすためで、重複防止の根拠ではない。fire 行は `fire_retention_seconds`（既定 30 日、`max_catchup_seconds` より長いことを設定検証で強制）保持するので、再計算されうる時刻の行は消えていない。

### 3. webhook（`generic-hmac`）

`POST /v1/hooks/{trigger_id}`（bearer token なし）。順序:

| # | 検査 | 失敗時 | 永続化 |
|---|---|---|---|
| 1 | trigger が存在し、webhook で、削除されていない | 404 `not_found`（`trigger not found`。未知・削除済み・cron・形式不正の id で同じ本文） | なし |
| 2 | body の大きさ: `Content-Length` が trigger の `max_body_bytes` を超えれば読まずに拒否、無いか偽りなら読みながら上限で打ち切る | 413 `payload_too_large` | なし |
| 3 | `x-tachyon-webhook-timestamp`（10 進の Unix 秒）が gateway の時計から `tolerance_seconds`（既定 300、上限 `webhook_max_tolerance_seconds`）以内（過去・未来の両方） | 401 `unauthorized`（`timestamp_outside_tolerance` / `missing_timestamp` / `malformed_timestamp`） | なし |
| 4 | `x-tachyon-webhook-signature: v1=<hex>`（`,` 区切りで複数可）のどれかが `HMAC-SHA256(secret, "{timestamp}.{raw body}")` に一致（定数時間比較、全 entry を比較） | 401 `unauthorized`（`signature_mismatch` / `missing_signature` / `malformed_signature`） | なし |
| 5 | trigger が有効 | 410 `gone`（署名が正しい request にだけ返す） | なし |
| 6 | event id（既定 header `x-tachyon-webhook-id`、trigger ごとに変更可）が 1..=128 の可視 ASCII | 400 `invalid_request` | なし |
| 7 | 同じ event id、または同じ署名（digest）の fire 行がある | 202（`replayed = true`、同じ invocation） | なし |
| 8 | tenant の認可（§1）→ 受付（§1） | 受付の拒否（409 / 413 / 429 / 503） | 受付時だけ |

- key は secret 文字列（`whsec_` + 64 hex）の UTF-8 bytes。署名の再計算は `openssl dgst -sha256 -hmac "$SECRET"` と一致する（`tsls triggers webhook-sign`、E2E で照合）。
- **replay**: 署名は timestamp と body しか覆わない（event id header は署名されない）。そのため tolerance 内に盗聴した署名済み request を event id だけ変えて再送されうる。これを防ぐために、検証した署名の SHA-256 を fire 行の一意 index（`(trigger_id, signature_digest)`）に持ち、同じ署名は同じ delivery として扱う。代償: 同じ秒に同じ body の別 event を送る sender は 2 件目を 1 件目の再送と見なされる（文書化した制約）。
- event id の dedup は `webhook_dedup_retention_seconds`（既定 7 日、`2 × webhook_max_tolerance_seconds` 以上を強制）保持する。それを過ぎた event id の再送は、署名 tolerance を満たす（= 新しく署名し直した）場合に限り新しい invocation になる。
- 再送の body が 1 回目と違っても、event id が同じなら 1 回目の invocation を返す（event id を正とする）。
- 存在の漏れ: 署名前に分かるのは「その id の webhook trigger が有効か無効で存在する」ことだけ（404 か、401 / 413 か）。trigger id は 80 bit の乱数を含む ULID で、URL として sender に渡すもの。無効化は署名済みの request にだけ 410 で知らせる（sender が再送をやめる合図）。削除後は未知の id と区別できない 404。
- event の形（関数に渡す JSON）: `{"source":"tachyon.webhook","trigger_id","trigger_name","event_id","content_type","body"}`。body が JSON でなければ `body_text`（UTF-8）か `body_base64`。timestamp と署名は渡さない。

### 4. secret の扱い

- 作成時（と `rotate_secret: true` の更新時）に gateway が 256 bit の乱数から生成し、**その応答でだけ** 返す（`Cache-Control: no-store`）。GET / list / PATCH（rotate 以外）は返さない。代わりに `secret_fingerprint`（`sha256:` + 12 hex、ドメイン分離ラベル付き）を返す。
- 台帳には AES-256-GCM で封じて `triggers.secret_sealed` に置く。鍵は `[triggers] secret_key_file`（64 hex、mode 0600）か `secret_key_env`。AAD は trigger id と tenant id（行を別 trigger / tenant にコピーすると復号に失敗する）。JSON の `body` 列には入らない。鍵が無い gateway では webhook trigger を作れない（400）。
- 削除で `secret_sealed` を消す（`secure_delete = ON`）。rotate で旧 secret は即座に検証に使われなくなる（猶予期間は無い）。
- secret binding（`[[secrets.bindings]]`）の参照は採らなかった: binding は環境への配送（`SecretDeliveryContext` に environment と epoch が要る）のための仕組みで、gateway 自身が検証に使う値の保管には合わない。

### 5. disable / delete と受付済みの扱い

- `PATCH {enabled: false}` と `DELETE` は trigger 行の generation を上げ、cron の `next_fire_at` を消す。**その commit より後に commit する fire は無い**: fire のトランザクションが trigger 行を読み直すので、scheduler が無効化の前に読んだ snapshot で fire しようとしても `Inactive` になる（`a_disable_or_delete_racing_a_scheduled_fire_commits_nothing`）。
- **受付済みの fire**（無効化の前に commit したもの）は普通の非同期 invocation で、trigger と独立に続く: outbox から publish され、PLT-4640 の dispatcher が実行・retry・DLQ を扱う。取り消したいときは invocation の cancel を使う。
- 再有効化は現在時刻の次から（無効だった期間を catch-up しない）。
- 関数の削除: 受付が 409 `function_deleted` になり、scheduler は refused を記録して trigger を無効化する。webhook は 409 を sender に返す。

### 6. data plane と複数 gateway

- trigger の CRUD・scheduler・webhook は management store（`state.db`）を持つ `combined` gateway でだけ有効（`[queue]` と durable 台帳も必要）。data plane では CRUD が 503 `control_plane_unavailable`、webhook が 503 `async_unavailable`（`not_configured`）。
- 同じ `state.db` の複数 gateway: scheduler lease で 1 つが回し、webhook はどの gateway でも受け付けられる（dedup は台帳の一意 index）。

## 結果（consequences）

- trigger 固有の retry・DLQ・quota は無い。受付の上限に当たった cron の時刻は deferred になり、長引けば policy で skip される（`trigger_fires_share_the_async_backlog_bound_and_retry_after_it_drains`）。
- 実行時の結果（成功・失敗）は trigger の fire 行ではなく invocation に残る。fire 行が持つのは「受け付けた / 恒久的に拒否した」だけ。
- 秒単位の 6 field 式を許すので、1 tenant が毎秒の trigger を `max_triggers_per_function` 個作れる。受付の outbox 上限は全 tenant 共通（T33）で、trigger ごとの最小間隔は設けていない（§14 に記録）。
- scheduler は gateway process 内の loop で、lease の TTL（既定 30 s）の間は owner の停止に気付かない。SIGKILL された owner の後任は、同じ host・instance の再起動なら即座に、別 gateway なら TTL 後に引き継ぐ。その間の時刻は missed run として policy に従う。
- 時計: 予定時刻の判定は gateway の時計に依存する。複数 gateway の時計がずれても一意キーで二重発火はしないが、発火の遅れ・早まりはずれの分だけ起きる。

## 検証

| 受入条件 | テスト / 記録 |
|---|---|
| 同じ予定時刻を再起動で重複作成しない | `services::triggers::tests::{a_restart_on_the_same_data_dir_never_fires_a_scheduled_time_twice, two_schedulers_on_one_ledger_fire_each_scheduled_time_once, a_cron_trigger_fires_each_scheduled_time_once_through_the_async_acceptance}`、E2E `restart.*`（SIGKILL + 停止期間、SIGTERM + 即時再起動） |
| fake clock・timezone 境界・missed run | `services::triggers::cron::tests::*`（NY の DST 開始・終了、Tokyo、日境界、Vixie の日付規則）、`services::triggers::tests::{the_scheduler_follows_the_trigger_time_zone_across_dst, missed_runs_follow_the_policy_after_downtime}` |
| invalid 署名・期限切れ・過大 body を永続受付前に拒否 | `services::triggers::webhook::tests::*`（署名 fixture）、`services::triggers::tests::webhook_refusals_happen_before_any_durable_write`、`apps/gateway/tests/triggers_http.rs::webhook_http_refuses_before_storing_and_dedups_resends`（Content-Length と streaming の 413）、E2E `webhook.*` |
| 再送 | `a_replayed_webhook_answers_the_same_invocation_and_creates_nothing`、E2E `webhook.duplicate_event_same_invocation`、`webhook.replayed_signature_new_event_id_same_invocation` |
| disable / delete 後の新規発火を止め、受付済みを説明できる | `disable_and_delete_stop_new_fires_and_accepted_invocations_continue`、`a_disable_or_delete_racing_a_scheduled_fire_commits_nothing`、`disabled_and_deleted_webhook_triggers_answer_without_leaking_existence`、E2E `disable.*` |
| quota / 認可 / Revision 固定 / 共通 retry 経路 | `a_cron_trigger_fires_each_scheduled_time_once_through_the_async_acceptance`（mode async・revision・key・outbox）、`trigger_fires_share_the_async_backlog_bound_and_retry_after_it_drains`、`a_deleted_function_refuses_the_fire_and_disables_its_trigger`。retry / DLQ は PLT-4640 の経路（trigger 側にコードが無いことが根拠） |
| tenant 境界・secret 非開示 | `trigger_crud_never_crosses_a_tenant`、`a_webhook_secret_is_shown_once_and_never_stored_or_returned_in_plain`、`triggers_http.rs::trigger_http_crud_never_returns_the_secret_and_never_crosses_a_tenant` |

## 非対象

- source 別の webhook 検証（GitHub `X-Hub-Signature-256`、Stripe 等）、汎用 SaaS connector。
- trigger の実行結果の集計、fire ごとの通知、concurrency policy（前の fire が実行中なら skip 等。k8s CronJob の `Forbid` / `Replace`）。
- 1 回だけの予定（at）、interval（`@every`）、秒未満の周期。
- webhook の IP allowlist、mTLS。

## 参照

- ADR-0003（台帳）、ADR-0007（設定配信と認可 lease）、ADR-0008（queue / object）、ADR-0010（invokeAsync と outbox）、PLT-4631（dispatcher lease）、PLT-4640（dispatcher・retry・DLQ）
- `docs/api.md` §5.11、`docs/cli.md`、`docs/architecture.md` §4「trigger（PLT-4641）」、`docs/threat-model.md` T45・T46・§14-17
