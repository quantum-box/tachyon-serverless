# ADR-0016: 予算は設定として配信し、実行開始前に最大料金を CAS で予約し、利用量 ledger の実測で精算する。予算が分からないときは新規受付を止める（PLT-4643）

## ステータス

Accepted（2026-09-17、PLT-4643）。**実決済はしない**。金額はすべて PLT-4642（ADR-0012）の**仮料金**で、単一 region・1 つの価格表の範囲の話である。

実装:

- 予算の設定と配信: `crates/application/src/budget/config.rs`（`BudgetConfig`、`BudgetBook`、`BudgetLimits`、`TenantBudget`）、`budget/publish.rs`（`BudgetPublisher`）、`control/wire.rs`（`ConfigKey::Budget` / `ConfigValue::Budget`）、`control/source.rs`（`LedgerConfigSource::with_budgets`）、`control/cache.rs`（`ConfigCache::budget`、`BudgetEntry`）
- 最大料金: `budget/charge.rs`（`max_charge`、`RunBounds`、`check_table`）
- 予約台帳: `budget/store.rs`（`BudgetStore`、`<data_dir>/usage/budget.db`）
- 受付・再確認・精算・失効・報告: `budget/mod.rs`（`BudgetService`）
- 受付の順序: `services/invoke.rs`（`InvokeService::invoke`、`Driver::execute` の再確認、`Driver::run` の `finish`、`BudgetRun`）、`services/admission/mod.rs`（`AdmissionController::precheck`）
- 精算の起動: `app.rs::Application::collect_usage`（collector の後に毎回 `settle_ready`）
- collector 停止の failpoint: `usage/mod.rs`（`COLLECTOR_PAUSE_FILE`、dev profile のみ）
- API / CLI / 運用: `GET /v1/budget`（`apps/gateway/src/handlers.rs::budget_report`）、`/readyz` の `budget`、`GET /metrics` の `tsls_budget_*`、`tsls budget`（`apps/cli/src/commands/budget.rs`）
- 試験: `crates/application/src/budget/tests.rs`、`crates/application/tests/budget.rs`、`apps/gateway/tests/gateway_integration.rs::a_data_plane_enforces_delivered_budgets_and_reports_them_per_tenant`
- E2E: `scripts/usage/budget-e2e.sh`、証跡 `docs/evidence/budget-*-process/`

## コンテキスト

- PLT-4642 までで、host が測った `AttemptSettled` を journal → collector → ledger に運び、version 付き価格表で仮料金を出せる。しかしそれは**実行した後に**見えるだけで、検証用の予算で実行を止める手段が無い。
- 受入条件（Linear PLT-4643）: 並列 Invoke で予約の合計が上限を突破しない・終了後に過剰予約を返す／不明な計測・lease 期限切れ時の新規受付方針を fail-closed で定義する／拒否理由を quota・budget・capacity で分け、開始済みの処理は定義済み deadline まで追跡する／alert と停止を別設定にし、予約・精算・取消の重複で残高を壊さない。
- 制約: 利用量は collector を経て遅れて ledger に届く（`collect_interval_ms`）。実行中の料金は分からない。ledger は `event_id` で重複を捨てるが、予算の残高は別に守る必要がある。PLT-4640（非同期 dispatcher）の各 run も同じ admission（`InvokeService::run_async`）を通る。

## 選択肢

| 案 | 内容 | 採否 |
|---|---|---|
| A | 実行開始前に**最大料金**を予約し（残高と同じトランザクションの CAS）、ledger に届いた実測で置き換える | **採用** |
| B | 実行後に ledger の合計だけを見て、超えていたら以後を止める | 不採用。collector の遅れの間に並列実行が上限をいくらでも超える（受入条件 1 に反する） |
| C | 実行中に定期的に使用量を見積もって途中で止める | 不採用。止めるには推測値が要り（ADR-0012「推測で請求しない」に反する）、実行中の処理を殺すのは受入条件 3「開始済み処理は deadline まで追跡」に反する |
| D | 予約を `state.db` の migration（009）に置く | 不採用。予算は usage の経路に属し、ledger と同じ障害領域・保持に置く方が単純。migration 番号は他 Issue（PLT-4640 の 007 等）と衝突しやすい |
| E | 予算を admission の in-memory 状態（`AdmissionState`）に持つ | 不採用。同じ `data_dir` の 2 gateway で共有できず、再起動で消える |

## 決定

### 1. 予算の形と配信（config cache 経由）

- 期間は **UTC の暦月**（`period = "calendar_month_utc"`、キーは `YYYY-MM`）だけ。rolling window は採らない（境界が決定的で、E2E と報告が単純）。予約は**予約した時刻の月**に属し、月をまたいで終わった実行も予約した月で精算する。
- 金額は PLT-4642 の価格表と同じ**通貨の 10⁻⁶ 単位の整数**（`*_micros`）。
- scope は tenant と、任意で function（`[[tenants.functions]]`）。scope ごとに**別の設定**:
  - `soft_limit_micros` + `alert_thresholds_percent`（例 `[50, 80, 100]`）: **alert だけ**。閾値を初めて越えた精算で 1 回（scope × 期間 × 閾値で一意）、warn log・`tsls_budget_alerts_total`・`GET /v1/budget` の `alerts_fired` に出す。何も止めない。
  - `hard_limit_micros`: **停止**。無ければ止めない。`alert_thresholds_percent` は `soft_limit_micros` 無しでは設定エラー。
- alert は**消費**（settled + unmetered hold）で判定し、予約は含めない（予約の大半は精算で返るため、含めると誤報になる）。停止は**確約**（reserved + settled + unmetered hold）で判定する。
- 設定: control plane（`combined`）の `[budget]` の inline（`[[budget.tenants]]`、`[budget.default_tenant]`）か `[budget] file`（同じ key を top level に持つ TOML）。file は publication のたびに size / mtime の変化を見て読み直す（`LedgerConfigSource::change_marker` にも入るので、in-process の cache は次の request で refresh する）。**壊れた file は採用せず、最後に有効だった予算を配信し続け**、`/readyz` の `budget.publication.last_error` に出す。
- 配信: `ConfigKey::Budget{tenant_id}` → `TenantBudget{tenant_id, period, currency, price_table_version, limits, functions}`。grant / tenant と同じ **auth lease**（`is_authorization`）で失効し、generation は ADR-0007 のとおり内容が変わったときだけ上がる（単調、巻き戻らない）。entry が無い（未配信・tombstone）tenant、`default_tenant` も無い tenant は予算不明として扱う。
- data plane は `[budget] enabled = true` のときだけ強制する（既定 off、P1〜P3 の既存構成は変わらない）。配信された `price_table_version` / `currency` が自分の価格表と違えば単位が合わないので予算不明として拒否する。

### 2. 予約（reserve）・精算（settle）・返却（release）・失効（expire）

**予約の単位（run）。** ledger の attempt id は admission の後で作られるので、予約のキーは admission の時点で決まる **run id** にする: 同期 invoke は invocation id、非同期（PLT-4640、`run_async`）の各 run は `<invocation id>:run-<attempt_base>:<ulid>`（admission 前に defer された run は attempt_base が進まないので、run ごとに一意にする）。1 run の中の attempt（warm の未配達 → cold 1 回）は run の予約で覆い、driver は `AttemptSettled` を出した attempt id の列を run の終わりに記録する。

**最大料金**（`budget/charge.rs::max_charge`、PLT-4642 と同じ `rating::charge` の丸め）:

| 量 | 上限 |
|---|---|
| billable ms | 価格表の `billable_segments` ごとの上限の和 × attempt 数、ただし run window + cancel grace で頭打ち、+ `reservation_slack_ms`（既定 250）。`queue_wait_ms` ≤ queue timeout、`vm_base_boot_ms` ≤ run window、`user_init_ms` ≤ 初期化 timeout + handshake timeout、`handler_ms` ≤ 実行 timeout + cancel grace。attempt 数は `user_init_ms` / `handler_ms` だけが課金区間なら 1（未配達の warm attempt はどちらも 0）、それ以外は 2 |
| vCPU-ms / MiB-ms | billable ms × revision の要求 `cpu_millis` / `memory_mib` |
| 転送 bytes | request bytes × 2 + `max_response_bytes` |
| invocation | 1 |

`teardown_ms` / `idle_pooled_ms` は予約時に上限が分からない（deadline 後の後始末、claim 前の idle）ので、それを課金する価格表は `[budget] enabled` で設定エラー（`check_table`）。

**状態機械**（`budget/store.rs`）: `reserved → settled | released | expired`。すべて `<data_dir>/usage/budget.db` の `BEGIN IMMEDIATE` 1 トランザクション:

- **reserve**: 同じ run id の行があれば何もせず今の状態を返す（冪等）。無ければ期間の tenant / function の totals を読み、`committed + max > hard_limit` なら拒否（`refusals` を数える）、そうでなければ行を挿入し totals の `reserved` を足す。読みと書きが同じ write lock の中なので、**別接続・別プロセスの並列予約でも合計は上限を超えない**（compare-and-set）。
- **finish**（run の終わり、driver）: `AttemptSettled` を出した attempt id の列、panic なら `complete = false`、journal の head seq（この run の event はすべてそれ以下）を記録。attempt が無く complete なら（queue で拒否された等）その場で 0 で精算する。
- **settle**（collector の後、`settle_ready`）: finish 済みの行について、ledger から attempt id ごとの `AttemptSettled` を読み、全部揃ったか、journal の cursor が head seq を越えた（= もう届かない、unjournaled）ら、その event だけを PLT-4642 の rating で 1 行に集計して置き換える:
  - 揃っていて全量が計測済み（`unmetered.attempts = 0`、`unmetered.bytes = 0`）→ `settled = 実測`、差額は返る。
  - 欠け・`unknown`・`guest_reported` がある → `settled = 計測できた分`、`held = reserved − settled` を **unmetered hold** として残す（**計測値より下に返さない・推測値を課金にしない**）。
  - 実測が予約を超えた → `settled = 実測`（全額）、`overrun_micros` に記録。
- **release**: 受付の途中で admission（quota / queue / capacity）が拒否した・idempotency key の競合に負けた run。**finish していない**行だけ。
- **expire**: finish しないまま `expires_at = run deadline + expiry_grace_seconds`（既定 30 s）を過ぎた行（gateway の kill -9、driver の消失）。**最大料金をそのまま unmetered hold** にする（fail closed）。実行が実際にはもっと短くても返さない。
- 遷移はすべて `UPDATE … WHERE state = 'reserved'` で、重複・遅着の呼び出しは何も変えずに今の状態を返す。totals は行と同じトランザクションで動き、列に `CHECK (… >= 0)` があるので、二重返却のような誤りは commit されずに失敗する。`verify_totals` で行の和と一致することを試験する。
- 精算額は run ごとに成分ごとに丸めるので、`GET /v1/usage` の行（tenant × function × 日で 1 回丸め）とは run 1 件あたり成分数 ×½ micro-unit 以内でずれうる（E2E 4b で確認）。

### 3. 受付の順序と拒否理由

同期 invoke（`InvokeService::invoke`）は台帳に何も書く前に次の順で判定する:

1. 設定 cache（認証・解決・policy、ADR-0007）
2. usage journal（ADR-0012、`usage_journal_full`）
3. **static admission**（`AdmissionController::precheck`: placement、削除中の function、breaker、node に決して載らない資源、tenant quota 0）。拒否は `placement` / `circuit_open` / `capacity` / `quota` で、**予算を予約しない**
4. **budget**（予約。拒否は `reason = budget`）。**capacity の grant を取らない**（queue にも入らない。`arrivals` も数えない）
5. **dynamic admission**（fair queue、tenant quota の持ち分、queue 上限、capacity）。拒否は `quota` / `queue_full` / `capacity`。拒否されたら予約を release
6. queue で待った run は grant を受けた時点で**予算を再確認**する（`BudgetService::recheck`: 配信が失効していないか、下げられた上限が自分を含む確約を認めるか）。拒否なら grant を返し、何も起動せずに invocation を `Failed{platform_error, Host.BudgetExhausted | Host.BudgetUnknown | Host.BudgetStoreUnavailable}` にし（HTTP は下の code）、run は 0 で精算される

| 拒否 | HTTP | `code` | `reason` | `error_type` |
|---|---|---|---|---|
| hard limit が最大料金を認めない | 429 | `budget_exhausted` | `budget` | `Host.BudgetExhausted` |
| 予算が不明（未配信・tombstone、auth lease 切れ、価格表の不一致、collector 停止） | 503 | `budget_unavailable` | `budget` | `Host.BudgetUnknown` |
| 予算 store が開けない・書けない | 503 | `budget_unavailable` | `budget` | `Host.BudgetStoreUnavailable` |

429 を選んだのは quota / capacity の 429 と同じ「今は受け付けない」の分類で client の扱いが揃うため。402 は決済を連想させるが、この prototype は決済をしない。`Retry-After` は付けない（上限か期間が変わるまで通らない）。

### 4. fail closed

`[budget] enabled` の gateway は次のとき新規 invocation を拒否する。**開始済みの実行は止めない**（deadline まで driver が追い、finish し、精算・失効で追跡される）:

- 予算 entry が無い・auth lease で失効した・価格表が違う → `Host.BudgetUnknown`
- **collector 停止**: finish 済みで未精算の run のうち最古のものが `max_unsettled_age_seconds`（既定 30 s）を越えた → `Host.BudgetUnknown`（`cause = collector_stalled`）。予約は実測が届くまで最大額のまま残るので残高は安全側だが、実測が届かない状態で受付を続けると予約が溜まり続け、予算の値そのものが分からなくなるため止める。collector が追いつき精算されれば自動で再開する。`/readyz` は 503、`budget.collector_stalled = true`
- 予算 store が使えない → `Host.BudgetStoreUnavailable`（次の操作で開き直す）

dev profile では `<data_dir>/usage/collector.pause` が存在する間 collector が配送しない（E2E 用。production profile では無視）。

### 5. 設定変更

- 上限の上げ下げは**以後の受付**に効く（配信が届いた時点。combined の gateway は次の request、data plane は次の refresh）。
- 現在の確約より下げても**実行中の run は止めない**。新規は `budget_exhausted`、queue で待っている run は grant 時の再確認で拒否される（どの waiter を残すかは選ばない。確約が新しい上限を超えている間はすべて拒否）。
- generation は ADR-0007 のとおり単調で、古い配信で上限が巻き戻らない。

### 6. API・CLI・運用

- `GET /v1/budget?period=YYYY-MM`（`invoke` role、token の tenant だけ。operator role は 403）: `enabled`、`provisional = true`、`billing_enabled = false`、`notice`、期間と境界、通貨・価格表 version、配信状態（`valid` / `expired` / `not_delivered`）と generation、`admitting` / `refusal`、tenant と function ごとの `soft_limit_micros` / `alert_thresholds_percent` / `hard_limit_micros` / `reserved_micros` / `settled_micros` / `unmetered_hold_micros` / `committed_micros` / `remaining_micros` / `overrun_micros` / 件数 / `alerts_fired`、そして `guarantee`（下の §7 の文）。
- `tsls budget [--period YYYY-MM]`: 1 行目は常に `PROVISIONAL - ...`。
- `/readyz` の `budget`: `enabled`、`accepting`、`refusal`、期間、価格表、store（健全性・path・統計・最後のエラー）、`collector_stalled`、`oldest_unsettled_age_seconds`、閾値、このプロセスの counters、publication（file、再読込回数、最後のエラー）。tenant の情報は含めない。`accepting = false` なら 503。
- `GET /metrics`: `tsls_budget_{enabled,store_healthy,collector_stalled,active_reservations,unsettled_runs,oldest_unsettled_age_seconds}`、tenant label（`[metrics] max_tenant_series` で上限、確約の大きい順）の `tsls_budget_{reserved,settled,unmetered_hold,remaining}_micros`、`tsls_budget_refusals_total{reason,cause}`、`tsls_budget_recheck_refusals_total`、`tsls_budget_transitions_total{result}`、`tsls_budget_alerts_total{scope}`、`tsls_budget_overrun_micros_total`、`tsls_budget_reservations_total`（`docs/metrics.md` §3.10）。

### 7. 金額の保証の範囲（明示）

- **対象**: PLT-4642 の仮料金だけ — `AttemptSettled` の host 計測の課金区間、要求 vCPU / memory、転送 bytes、invocation 数。
- hard limit が抑えるのは `reserved + settled + unmetered hold`。各 run は起動前に最大料金を予約するので、**overrun が無い限り確約は上限を超えない**。overrun（最大の見積りを実測が超えた分）は全額精算して数えるので、確約が上限を超える量は overrun の合計以下。
- 計測できなかった部分は unmetered hold として上限に数えるが、**課金額ではない**（`settled_micros` に入らない）。
- **対象外**: host の原価（pool の idle、teardown、attempt の無い boot、cgroup CPU）、PLT-4642 が rate しないもの、期間の途中の価格表の変更、複数 region、実決済・請求書。
- 1 つの region・1 つの `data_dir`（同じ store を共有する gateway 群）の中だけ。別の `data_dir` を持つ gateway 同士は予算を共有しない。

## 結果（consequences）

- invoke 1 件に予算 store の transaction が 2〜3 回（stats、reserve、finish）と精算 1 回増える。fsync（`synchronous = FULL`）を伴う。process provider の試験・E2E で目立つ遅延は無いが、Firecracker 上の影響は**未計測**。
- 最大料金は timeout・初期化 timeout・最大 response size から出すので、短い処理でも予約は大きい（E2E の 0.1〜1 s の実行でも、実行 timeout 2 s + 初期化 timeout + handshake timeout 分の vCPU / memory と 6 MiB の転送を予約する）。上限が小さいと、実際の消費よりずっと少ない並列数で `budget_exhausted` になる。精算で返るまで（collector 間隔 + 精算）次の受付に使えない。
- collector が止まると `max_unsettled_age_seconds` 後に**全 tenant** の新規受付が止まる（予算を強制する gateway の可用性より、予算の正しさを取る）。
- 予算 store を共有しない gateway（別 `data_dir`）が同じ tenant を受け付けると、上限はそれぞれに効く（合算されない）。
- 配信に `kind = "budget"` の entry が加わる。この変更より古い data plane は予算を配る control plane の配信を解釈できず refresh に失敗する（設定の TTL 後に受付を止める側に倒れる）。control plane より先に data plane を更新する。
- kill -9 された gateway の run は `AttemptSettled` を出さない（ADR-0012）ので、予算では deadline + grace の後に最大額の hold になり、期間の終わりまで返らない（実際より多く数える側）。
- 非同期 invoke（PLT-4640）の run も `run_async` で precheck → reserve（window は run の deadline まで、queue timeout は `admission_wait`）→ admit の順に通り、driver の `BudgetRun` で同期と同じ再確認・finish・精算が働く。予算による拒否（`Host.BudgetExhausted` / `Host.BudgetUnknown` / `Host.BudgetStoreUnavailable`）は何も起動する前なので、dispatcher は attempt を数えずに **defer** する（`invoke_async::retry::classify`）。defer は event の最大 age で有界で、予算が戻らなければ age で dead letter になる。

## 検証

| 観点 | 試験 |
|---|---|
| 並列で上限を超えない | `budget::tests::concurrent_reservations_on_separate_connections_never_exceed_the_limit`（16 thread × 別接続）、`tests/budget.rs::parallel_invocations_never_reserve_beyond_the_hard_limit_and_the_excess_returns`（12 並列、4 件分の上限）、E2E 3a / 3b |
| 過剰予約の返却と ledger との一致 | 同上、`budget::tests::a_reservation_is_refused_when_it_does_not_fit_and_settlement_returns_the_excess`、E2E 4a〜4d・5 |
| timeout / cancel / 消えた run | `tests/budget.rs::{timeout_and_cancel_settle_at_their_measured_charge, a_run_that_never_reports_back_expires_to_an_unmetered_hold}`、`budget::tests::a_finished_run_is_never_released_or_expired_and_an_unfinished_one_expires_to_a_hold`、`an_incomplete_measurement_holds_the_rest_and_an_overrun_is_settled_in_full` |
| collector 停止 → fail closed → 再開 | `tests/budget.rs::a_stalled_collector_fails_closed_and_admission_resumes_after_it_catches_up`、E2E 7a〜7c・8 |
| 設定更新（下げる・上げる、実行中は継続、待機中は再確認） | `tests/budget.rs::{lowering_a_limit_stops_new_work_but_not_running_work_and_raising_resumes, a_queued_invocation_rechecks_the_budget_when_it_is_granted}`、`budget::tests::a_lowered_limit_fails_the_recheck_of_a_queued_reservation`、E2E 9 |
| 重複・順序の入れ替わりで残高が壊れない（property） | `budget::tests::property_duplicate_and_interleaved_transitions_never_break_the_balance`（300 round、重複 1〜3 回）、`duplicates_are_no_ops_and_terminal_states_never_move` |
| 予約が実測を覆う（property） | `budget::tests::property_the_reservation_covers_every_rating_within_the_bounds`（5 000 件、warm 未配達 + cold retry を含む）、`the_maximum_follows_the_documented_formula` |
| alert と停止の分離 | `tests/budget.rs::alerts_fire_without_stopping_and_the_hard_limit_stops_without_alerts`、`budget::tests::alerts_fire_once_per_threshold_and_never_refuse`、`alerts_and_stop_are_separate_settings_and_validated`、E2E 6 |
| 拒否理由と順序 | `tests/budget.rs::quota_budget_and_capacity_refusals_are_distinct_and_ordered` |
| tenant 境界・未配信は fail closed | `tests/budget.rs::budgets_are_tenant_isolated_and_an_undelivered_budget_fails_closed`、gateway `a_data_plane_enforces_delivered_budgets_and_reports_them_per_tenant`、E2E 10 |
| 非同期 run の予約と defer | `invoke_async::dispatch_tests::asynchronous_runs_reserve_budget_and_a_refused_run_is_deferred` |
| store 停止 | `tests/budget.rs::an_unavailable_budget_store_refuses_with_its_own_error_type`、`budget::tests::an_unavailable_store_refuses_every_operation_and_recovers` |
| data plane への配信 | gateway `a_data_plane_enforces_delivered_budgets_and_reports_them_per_tenant`（HTTP の配信、tombstone で `Host.BudgetUnknown`） |

## 非対象

- 実決済・請求書・前払い credit の購入・返金・異議申立て。
- rolling window、日次の上限、複数通貨・複数価格表の同時適用、region をまたぐ予算の共有。
- 実行中の処理の強制停止（上限に達しても開始済みの run は deadline まで走る）。
- 予算 store の複製・署名（ledger と同じく local file、threat-model T50 / §14-18）。
- 非同期 invoke の受付（`invokeAsync` の 202）時点の予約。受付は予算を見ず、実行する run だけが予約する。
- Firecracker 実機での遅延の測定。

## 参照

- RFC（quantum-box/knowledge PR #284）§16 課金・原価・予算上限
- ADR-0006（admission）、ADR-0007（設定配信と認可 lease）、ADR-0010（非同期受付）、ADR-0012（利用量 ledger と仮料金）
- `docs/threat-model.md` T50〜T54、§14-18
