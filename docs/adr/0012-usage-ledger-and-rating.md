# ADR-0012: 利用量は host が測った UsageEvent v2 を上限付きの durable journal に同期で書き、collector が at-least-once で event_id 重複排除の ledger へ運び、version 付き価格表で仮料金を集計する（PLT-4642）

## ステータス

Accepted（2026-09-17、PLT-4642）。**実請求はしない**。`[usage.billing] enabled = true` は設定エラーで、請求・決済・請求書送付のコードはこの repository に無い。

実装:

- event: `crates/domain/src/usage.rs`（`UsageEvent` schema v2、`Metered` / `Measurement`、`UsageSegments`、`UsageResources`、`UsageBytes`、`GuestReportedUsage`、`AttemptKind`、`UsageOutcome`）
- 計測点: `crates/application/src/services/invoke.rs`（`AttemptMeter`、`emit_attempt_settled`、`emit_environment_abandoned`、`usage_resources`）、`crates/application/src/services/pool.rs`（`emit_stopped`、`PooledSession.pooled_at`、`WarmEnvironment.idle_ms`）
- host の resource 値: PLT-4637 の `ExecutionProvider::environment_stats`（`GET /metrics` と同じ読み取り口）を terminate の直前に 1 回読む（`crates/application/src/services/invoke.rs::{sample_before_terminate, usage_resources}`）。別の reader は足していない
- metrics: `crates/application/src/metrics/{catalog.rs, render.rs}` の `tsls_usage_*`（`docs/metrics.md` §3）
- journal: `crates/application/src/usage/journal.rs`（`UsageJournal`、`<data_dir>/usage/journal.db`）
- ledger: `crates/application/src/usage/ledger.rs`（`UsageLedger`、`<data_dir>/usage/ledger.db`）
- collector・admission・報告: `crates/application/src/usage/mod.rs`（`UsageMeter`、`JournalingUsageSink`）
- 仮料金: `crates/application/src/usage/rating.rs`（`PriceTable`、`rate`、丸め）
- 設定: `[usage]`（`crates/application/src/usage/config.rs`）
- API / CLI: `GET /v1/usage`（`apps/gateway/src/handlers.rs::usage_report`）、`/readyz` の `usage`、`tsls usage`（`apps/cli/src/commands/usage.rs`）
- 試験: `crates/application/src/usage/tests.rs`、`crates/application/tests/usage.rs`、`crates/application/tests/pipeline.rs::a_retry_is_metered_apart_from_the_first_attempt`、`apps/gateway/tests/gateway_integration.rs::usage_report_is_provisional_tenant_scoped_and_fails_closed`
- E2E: `scripts/usage/usage-e2e.sh`、証跡 `docs/evidence/usage-*-process/`

## コンテキスト

- PLT-4632 までで `UsageEvent`（v1）は driver と pool から `InMemoryUsageSink` に送られ、環境ごとの sequence、event id の重複排除、環境 1 つにつき `EnvironmentStopped` 1 回、が保証されていた。しかし:
  - **プロセスのメモリにしか無い。** 再起動・kill -9 で消え、上限も無い。
  - **何の attempt の、どの区間の、誰が測った値か** が event から分からない。v1 は `HandlerFinished.monotonic_duration_ms`（host）と `EnvironmentStopped` の環境寿命（台帳の wall clock の差）だけで、queue 待ち・VM 起動・user init・teardown・pool の idle を分けていない。retry と初回、timeout と失敗も区別しない。
  - 初期化に失敗した環境（boot / handshake / init）、dispatch 前の client deadline、slot 取得失敗では `EnvironmentStopped` を出していなかった（計測漏れ）。
  - 価格表・原価・利用量の境界が無い。`UsageSummaryResponse.not_billable = true` だけ。
- RFC §15–16: UsageEvent は「ホスト / 入口の journal → durable stream → ledger」。guest の自己申告を請求根拠にしない。同一 event id を一度だけ集計。journal の disk full・停止を検知し、保存できないなら新規実行を止めるか無課金に固定し、欠けた区間を推測で請求しない。platform の queue 待ち・image 配布・VM 基底起動を利用者コードの実行時間に混ぜない。build 料金は既存 build metering と区別する。

## 選択肢

| 案 | 内容 | 採否 |
|---|---|---|
| A | 計測点で local の上限付き journal（SQLite、fsync）に同期 append → collector が ledger へ at-least-once、ledger は `event_id` 主キーで重複排除 | **採用** |
| B | 計測点で `state.db` の invocation 完了トランザクションに usage 行を同時に書く | 不採用。usage が control plane の台帳と同じ write lock・同じ保持・同じ障害領域に入る。RFC §15「UsageEvent を一件ずつ無制限に TiDB へ蓄積しない」に反し、regional ledger への分離（将来の配置）が遠のく。migration 番号も他 Issue と衝突しやすい |
| C | JetStream（PLT-4638）に直接 publish | 不採用（今は）。broker 停止で計測が止まり、ACK / dedup window は kill -9 をまたいで保証されない（ADR-0008 追記）。journal → stream は将来 collector の送り先を替えるだけで足せる |
| D | 計測は in-memory のまま、定期的に ledger へ flush | 不採用。flush 前の kill -9 で失う。「計測できないまま実行した」区間が区別できない |

## 決定

### 1. UsageEvent v2（`meter_version = 2`）

v1 の field はそのまま残し（既存の履歴 API・試験が読む）、次を足す。v1 の event も読める（新しい field はすべて「不明」になる既定値を持つ）。

| field | 内容 |
|---|---|
| `event_id` | 決定的: `<environment>:<epoch>:<sequence>`（pool が終わらせた環境は `<environment>:<epoch>:pool-stopped`）。再送しても同じ id |
| `tenant_id` / `function_id` / `revision_id` / `invocation_id` / `attempt_id` | 帰属。pre-start（`min_ready`）には invocation が無い |
| `attempt_number` / `attempt_kind` | 台帳の attempt 番号と `first` / `retry`（今の retry は warm 環境に `Invoke` が届かなかったときの cold 1 回だけ。PLT-4640 の再配送もこの field で区別する） |
| `outcome` | `succeeded` / `failed` / `timeout` / `cancelled` / `outcome_unknown`。完了の書き込みが fenced で拒否された attempt（他 dispatcher が回収）は `outcome_unknown` |
| `environment_id` / `epoch` / `sequence` / `boot_id` | `boot_id` は guest の `Hello.guest_boot_id`（guest 申告。照合用で、何も証明しない） |
| `observed_at` / `wall_clock_source` | host の wall clock（注入された `Clock`）。**日付への振り分けにだけ使う** |
| `segments` | `queue_wait_ms` / `vm_base_boot_ms` / `user_init_ms` / `handler_ms` / `teardown_ms` / `idle_pooled_ms`。host の monotonic clock（`Instant`）で測り、**ms に切り上げ**。起きなかった区間は `host(0)`、起きたが測っていない区間は `unknown` |
| `resources` | 要求値（revision の `cpu_millis` / `memory_mib` / `ephemeral_storage_mib`）と、terminate の直前に `ExecutionProvider::environment_stats` で読んだ host の CPU 秒（usec に換算）と peak memory。`scope = cgroup_v2`（Firecracker の VMM cgroup、guest + VMM）なら `provider_reported`。process provider の `procfs` / `proc_pid_rusage` は bridge プロセスだけで user process を含まず過少なので `unknown`。読めなければ `unknown`。terminate の直前の値なので、terminate 中に使った分は入らない（原価の参考値） |
| `bytes` | gateway が数えた request / response の JSON bytes（`host_measured`） |
| `guest_reported` | `Ready.init_ms`、`Response.handler_ms`。**記録するが、rating は読まない** |

各量は `Metered { value, measurement }` で、`measurement` は `host_measured` / `provider_reported` / `guest_reported` / `unknown`。rating が使うのは前の 2 つだけ（`Metered::ratable_value`）。

区間の定義（cold / warm）:

| 区間 | cold | warm |
|---|---|---|
| `queue_wait_ms` | 受付 → capacity grant | 同じ |
| `vm_base_boot_ms` | provider の create 開始 → bridge 接続（`EnvironmentHandle.created_at → connected_at`） | pool からの resume + readiness check |
| `user_init_ms` | bridge 接続 → `Ready`（handshake を含む） | 0 |
| `handler_ms` | `Invoke` frame の書き込み → 結果 / timeout / cancel。frame が届かなかった attempt は 0 | 同じ |
| `teardown_ms` | 結果の後 → terminate 完了（terminate が失敗したら `unknown`）。pool に渡した場合は渡すまで | 同じ |
| `idle_pooled_ms` | 0 | 直前に pool に入ってから claim まで。pool が終わらせる環境は `EnvironmentStopped` に、最後に pool に入ってから terminate まで |

計測点と event:

| event | 出す所 | 役割 |
|---|---|---|
| `AttemptSettled`（新） | driver: attempt の後始末の後（terminate 済み、または pool に渡した直後）、`EnvironmentStopped` の前。warm dispatch 失敗の retire でも出す | **rating が課金対象として読む唯一の event** |
| `EnvironmentStopped` | driver / pool（従来どおり環境 1 つに 1 回）。加えて、dispatch 前に終わった環境（boot・handshake・init の失敗、初期化中の cancel、dispatch 前の client deadline、slot 取得失敗、invocation 消失）でも出す（`emit_environment_abandoned`、attempt 無し・`outcome` 付き） | 原価（環境寿命、idle、teardown、cgroup CPU / memory） |
| `EnvironmentStarted` / `HandlerStarted` / `HandlerFinished` | 従来どおり | 監査用。合算しない（`AttemptSettled` と二重になるため） |

`AttemptSettled` の sequence は pool に渡す前に予約する（`release_for` に `seq + 1` を渡す）ので、環境の sequence は pool をまたいでも単調のまま。

### 2. bounded durable journal（`UsageJournal`）

- `<data_dir>/usage/journal.db`（`state.db` とは別の SQLite。state を永続化しない構成では in-memory）。WAL、`synchronous = FULL`、file mode 0600。
- `JournalingUsageSink::record` が **計測点で同期に** 1 行 1 トランザクション（`BEGIN IMMEDIATE`）で append してから in-memory の view（履歴 API）に渡す。client への応答（driver の完了通知）は `AttemptSettled` の append の後。
- 表: `function_usage_journal(seq, event_id, tenant_id, body, body_bytes, chain, appended_at)`、`function_usage_journal_state(cursor_seq, cursor_chain, pending_events, pending_bytes)`、`function_usage_journal_losses(tenant_id, events)`。
- **上限**: 未回収の件数 `journal_max_events`（既定 100 000）と bytes `journal_max_bytes`（既定 64 MiB）。カウンタは同じ DB に行と同じトランザクションで更新するので、同じ `data_dir` の 2 つ目の gateway とも共有される。
- **満杯**: 上限を越える append は拒否し、tenant ごとの `unjournaled` として数える（黙って捨てない）。
- **新規受付の拒否（fail closed）**: 残りが `admission_headroom_events`（既定 1 000）/ `admission_headroom_bytes`（既定 1 MiB）以下になったら、新しい invoke を**台帳に何も書く前に** `503 usage_journal_full`（`reason = usage_journal_full`、`error_type = Host.UsageJournalFull`）で拒否する。headroom は受付済みの invocation が自分の event を書き切るための余白で、受付済みの実行は止めない。`min_ready` の先行起動も止める。冪等 key の replay（再実行しない）は答える。
- **停止（unavailable）**: 開けない・書けない（disk full、I/O error、read-only）journal は同じ方針で `reason = usage_journal_unavailable`（`Host.UsageJournalUnavailable`）。接続を捨て、次の admission / collector tick が開き直す（`probe`）。journal を起動時に開けなくても gateway は起動し、`/readyz` が 503 を返す（ledger を開けないのは local file の設定・権限の誤りなので起動エラー）。
- **開発用の例外**: `on_journal_full = "accept_unmetered"` は `profile = "dev"` でだけ有効（production は設定エラー）。拒否の代わりに受け付け、その invocation の event は量をすべて `unknown`・`evidence_quality = unknown` にし、journal に入らなかった分は `unjournaled` に数える。推測値は作らない。
- **改竄検出**: 各行に `chain = sha256(前の chain ‖ "\n" ‖ body)`。collector は cursor の chain から再計算し、合わない行で止まる（`/readyz` の `usage.collector.last_error` に `integrity`）。ファイル全体を書き換えられる者は chain も作り直せる（残存リスク、threat-model T36）。

### 3. collector と ledger

- collector（`UsageMeter::collect`、gateway の loop が `[usage] collect_interval_ms`（既定 1 s）ごとに `spawn_blocking` で実行、shutdown 時にもう 1 回）は cursor の後ろを `collect_batch`（既定 500）件ずつ順に読み、chain を検証し、ledger に 1 トランザクションで渡し、**ledger の commit の後に** cursor を CAS で進めて回収済みの行を消す。
- ledger（`UsageLedger`、`<data_dir>/usage/ledger.db`、表 `function_usage_events` / `function_usage_ledger_stats`）は `event_id` を主キーに `INSERT OR IGNORE`。したがって:
  - 計測点の再送（同じ event id）→ 1 行。
  - ledger commit 後・cursor 前の crash → 次の collector が同じ batch を再送し、重複として数えるだけ（`duplicates_ignored`）。
  - 遅れて届いた・順序の違う event → id で受け付ける。
  - 2 つの gateway の collector が同じ journal を読んでも、cursor の CAS に負けた側は読み直し、ledger は重複を捨てる。
- **時計**: 量はすべて monotonic の区間から。ledger は受け取った時刻と `observed_at` の差を `wall_clock_skew_ms` として記録するだけで、量の計算には使わない。`EnvironmentStopped.monotonic_duration_ms`（環境寿命）は台帳の wall clock の差なので**原価の参考値**で、rating は読まない。
- ledger は事実だけを持ち、価格・料金を持たない。prototype では gateway ごとの local file。将来は regional の ledger service（RFC §15）で、collector の送り先を替える。

### 4. 仮料金（rating）

- 価格表（`PriceTable`）: `version`、`effective_from`、`currency`、`billable_segments`、`unit_prices_micros`（`vcpu_second` / `gib_second` / `invocation` / `gb_transferred`、通貨の 10⁻⁶ 単位の整数）。`[usage] price_table = "<file>.toml"` か、組み込みの `provisional-dev-2026-09-v1`（JPY、`billable_segments = ["user_init_ms", "handler_ms"]`。**数値は丸めを見せるための仮置きで料金ではない**）。RFC §16.1 の `amount_milli_yen` より細かい単位にしたのは、1 ms 単位の丸めを 0 に潰さないため。
- 課金対象: `AttemptSettled` だけ。`invocations` は `attempt_kind = first` の件数（retry は別に数え、invocation 料金を二重に取らない）。compute は `billable_ms × 要求 cpu_millis`（vCPU-ms）と `billable_ms × 要求 memory_mib`（MiB-ms）、転送は request + response bytes。**queue 待ち・VM 基底起動・teardown・pool の idle は既定の価格表では課金しない**（RFC §16.2「platform の queue 待ち・image 配布・VM 基底起動を利用者コードの実行時間に混ぜない」）。価格表で区間を選べるが、`queue_wait_ms` 等を足すかは料金規約の判断で、この ADR では決めない。
- **不明は非課金**: `unknown` / `guest_reported` の区間は 0 として扱い、`unmetered`（区間ごとの件数と attempt 数）に出す。要求 resource が無い event（v1）も compute は 0 で `unmetered`。journal に入らなかった event は報告の `unjournaled_events`。
- **丸め（順に適用、`rating.rs` の property test で固定）**:
  1. 区間は monotonic clock から ms に**切り上げ**（1 µs の handler も 1 ms）。`Σ ceil(dᵢ) ≥ ceil(Σ dᵢ)`、差は 1 件あたり 1 ms 未満。
  2. attempt ごとの `billable_ms` は課金区間の正確な和。
  3. 行（tenant × function × day、`group_by` による）ごとに量を整数で正確に合計。
  4. 行・成分ごとに `round_half_up(量 × 単価 / 単位)` を 1 回だけ（event ごとには丸めない）。行の合計 = 成分の和、報告の合計 = 行の合計の和（再丸めしない）。
  - 帰結: 料金は負にならず、量が増えて減ることはない。1 行を n 行に分けても各成分の差は n/2 micro-unit 以内。
- 1 つの報告は 1 つの価格表で計算し、`from` が `effective_from` より前の範囲は 400。日付は event の `observed_at`（host wall clock、UTC）で決める。

### 5. 原価・利用量・価格の境界と、既存 build 課金との分離

| 区分 | 何か | どこに出るか |
|---|---|---|
| 利用量（usage） | host が測った attempt の量（区間 ms、要求 resource × 時間、bytes、件数・outcome） | `lines[].usage` |
| 価格（price） | version 付き価格表 × 利用量 = **仮**料金 | `lines[].provisional_charges_micros`、`price_table` |
| 原価（cost） | host が tenant の環境に使ったもの（環境寿命、pool の idle、teardown、attempt を持たなかった boot / init、cgroup CPU usec / peak memory） | `lines[].cost`。価格を掛けない |
| 不明 | 測れなかった区間・bytes、journal に入らなかった event | `lines[].unmetered`、`unjournaled_events` |
| guest 申告 | guest の `handler_ms` / `init_ms` | `lines[].guest_reported`。価格を掛けない |

- 表・module は `function_usage_*` / `crates/application/src/usage/`。**tachyon-apps の build 課金（build metering）とは別の pipeline** で、互いに読み書きしない（RFC §16.2、`docs/inventory-tachyon-apps.md`）。
- 請求は無効: `[usage.billing] enabled = true` は設定エラー、`usage::BILLING_ENABLED = false`、`GET /v1/usage` は常に `provisional = true`、`not_an_invoice = true`、`billing_enabled = false` と notice を返し、`tsls usage` の 1 行目は `PROVISIONAL - ...`。

### 6. API・CLI・運用状態

- `GET /v1/usage?from&to&group_by&function_id`: token の tenant だけ（`invoke` role、operator role は 403）。他 tenant の function id を指定しても空（存在の oracle にしない）。`group_by` は `function` / `day` / `function,day`（既定）/ `none`。範囲は既定で直近 31 日（価格表の `effective_from` より前には伸ばさない）、最大 92 日。
- `/readyz` の `usage`: `accepting`、`metered`、`policy`、`billing_enabled`、`price_table_version`、journal（健全性、未回収件数 / bytes、cursor、上限、`unjournaled_events`、最後のエラー）、collector（実行回数、最後の成功、エラー、配送 / 挿入 / 重複件数）、ledger（件数、無視した重複）。tenant の情報は含めない。`accepting = false` なら `/readyz` は 503。
- `tsls usage [--from] [--to] [--group-by] [--function]`、`--json` で本文そのまま。

### 7. metrics（PLT-4637 の catalog）

`GET /metrics` に `tsls_usage_journal_{healthy,admitting,pending_events,pending_bytes,max_events,max_bytes}`、`tsls_usage_unjournaled_events_total`、`tsls_usage_collector_{runs_total,failing,last_success_age_seconds,delivered_events_total}`（最後が collector の遅れ）、`tsls_usage_ledger_{events,duplicates_ignored_total}` を出す（`docs/metrics.md`）。tenant の label は持たない。

### 8. crash point

`TSLS_USAGE_CRASH_POINT=collector.after_ledger_commit` は **`profile = "dev"` のときだけ**有効で、collector が ledger に commit した直後・cursor を進める前にプロセス自身へ `SIGKILL` を送る（E2E の B 段）。production profile では環境変数があっても何もしない。

## 結果（consequences）

- 実行ごとに `AttemptSettled` と `EnvironmentStopped` の 2 行ほど（最大 5 行）の fsync が増える。process provider の E2E と application 試験で目立った遅延は無いが、Firecracker 上の latency 影響は**未計測**。
- journal が満杯・停止すると新規 invoke が 503 になる（可用性より計測の正しさを取る）。collector が止まった gateway は `journal_max_events - admission_headroom_events` 件の event を書いたところで受付を止める。
- 環境寿命（原価）は wall clock の差のまま。時計が飛ぶと原価の参考値は狂うが、利用量と仮料金は変わらない（試験 `wall_clock_skew_does_not_change_quantities`）。
- warm 環境の idle は次に claim した attempt の `AttemptSettled` か pool の `EnvironmentStopped` に載る（二重には載らない）。pool から取り出した後 retire した環境の idle は `unknown`。
- 非同期 invoke（PLT-4639）の受付は journal を見ない（まだ実行しない）。PLT-4640 の dispatcher は実行前に `UsageMeter::admit` を呼ぶこと。

## 検証

| 観点 | 試験 |
|---|---|
| known-duration sample | `tests/usage.rs::a_known_duration_handler_is_metered_by_the_host`（fake、300 ms）、E2E 8b / 8c（process provider、cpu-burn 1.0 s × 3、timeout 2 s、0.2 s） |
| retry と初回、timeout | `tests/pipeline.rs::a_retry_is_metered_apart_from_the_first_attempt`、`tests/usage.rs::a_timeout_is_recorded_as_timeout_with_the_host_measured_duration`、E2E 2 / 8a |
| 同一 event の再送・replay | `usage::tests::a_duplicate_event_is_a_single_ledger_row`、`tests/usage.rs::an_idempotent_replay_is_metered_once`、E2E 3 |
| collector 再起動 | `usage::tests::a_crash_between_the_ledger_commit_and_the_cursor_never_double_counts`、`tests/usage.rs::a_collector_restart_on_the_same_data_dir_never_double_counts`、E2E 5–7（kill -9 と SIGKILL crash point） |
| 時計のずれ | `tests/usage.rs::wall_clock_skew_does_not_change_quantities`、`usage::tests::late_and_out_of_order_events_are_accepted_by_id` |
| journal 満杯 / 停止 | `tests/usage.rs::a_full_journal_refuses_new_invocations_fail_closed`、`an_unavailable_journal_refuses_new_invocations`、`accept_unmetered_counts_what_it_could_not_meter`、`usage::tests::the_journal_bound_refuses_appends_and_counts_them_per_tenant`、`admission_keeps_headroom_for_work_already_admitted`、`a_journal_that_cannot_be_opened_is_unavailable_not_fatal` |
| 丸め property | `usage::tests::property_charges_are_monotonic_in_the_quantity`、`property_per_invocation_ceil_never_undercounts_the_sum`、`property_line_split_is_bounded_and_totals_are_sums`、`rating_follows_the_documented_formula` |
| guest 申告を使わない | `tests/usage.rs::guest_reported_times_never_reach_a_charge`、`usage::tests::unknown_and_guest_reported_segments_contribute_nothing_and_are_reported` |
| 原価と価格の境界 | `tests/usage.rs::provider_reported_cgroup_usage_is_cost_not_price`、`a_failed_initialization_is_accounted_without_an_attempt` |
| tenant 境界 | `tests/usage.rs::usage_reports_are_tenant_scoped`、`usage::tests::the_ledger_only_answers_for_the_callers_tenant`、gateway `usage_report_is_provisional_tenant_scoped_and_fails_closed`、E2E 9 |
| 改竄 | `usage::tests::a_tampered_journal_row_stops_collection` |
| 請求無効 | `usage::tests::billing_cannot_be_enabled_and_accept_unmetered_is_dev_only` |

## 非対象

- 実請求・決済・請求書・訂正・異議申立て、予算上限（soft / hard budget、credit の事前予約）。
- 多重化 HTTP（1 環境で並列）の active 区間の和集合。今の実行は 1 環境 1 invocation なので重ならない。
- durable stream（JetStream）経由の配送、regional ledger service、保持期間・archive。
- 保存量（object store）の課金。
- Firecracker 実機での cgroup CPU usec の検証（`environment_stats` を読む配線と fake での経路だけ。E2E は process provider）。

## 参照

- RFC（quantum-box/knowledge PR #284）§15 データ基盤、§16 課金・原価・予算上限
- ADR-0003（`state.db`）、ADR-0008（queue の ACK を決定の根拠にしない）、ADR-0010（outbox の at-least-once）
- `docs/threat-model.md` T06、T36、T37
