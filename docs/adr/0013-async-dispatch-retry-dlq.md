# ADR-0013: 非同期 invocation は台帳の dispatch 行を claim してから同期 invoke の経路で実行し、結果・次の試行・dead letter を 1 トランザクションで確定した後に ACK する。retry は outbox の次世代 event で行う（PLT-4640）

## ステータス

Accepted（2026-09-17、PLT-4640）。ADR-0010（受付と outbox）の続き。queue から取り出して実行し、retry・DLQ・redrive までが範囲。

実装:

- consumer / reaper: `crates/application/src/services/invoke_async/dispatch.rs`（`AsyncDispatcher::handle` / `run_once` / `reap`）
- retry policy と分類: `crates/application/src/services/invoke_async/retry.rs`（`[async_dispatch]`、`classify`、`RetryBudget`）
- 実行: `crates/application/src/services/invoke.rs`（`InvokeService::run_async`、driver の `async_run` sink）
- dead letter / redrive: `crates/application/src/services/invoke_async/dead_letters.rs`、`apps/gateway/src/dead_letters.rs`
- 台帳: `crates/application/src/repository/dispatch.rs`（`AsyncDispatchRepository`）、`crates/application/src/repository/sqlite/dispatch.rs`、migration `008_async_dispatch.sql`
- gateway の loop: `apps/gateway/src/lib.rs::serve`（worker × `workers`、reaper）
- failpoint: `dispatch.after_claim` / `dispatch.before_commit` / `dispatch.before_retry_commit` / `dispatch.after_commit`（`crates/application/src/failpoints.rs`）
- sample: `examples/idempotent-async`
- E2E: `scripts/queue/async-dispatch-e2e.sh`、証跡 `docs/evidence/async-dispatch-e2e-*/`

## コンテキスト

- ADR-0010 で非同期 invocation は `queued` まで進む。queue の message は invocation id を指すだけで、入力と固定 Revision は台帳にある。
- ADR-0008 の追記のとおり、JetStream の dedup 表と配送回数（`max_deliver` の数え）は kill -9 をまたいで保証されない。retry 回数を broker に数えさせると、broker の crash で数え直しになり上限が効かない。
- 同期 invoke の driver（PLT-4631）は、invocation を dispatcher に所有させ（`invocations.owner_id`）、終わった時点で invocation を terminal にする。dispatcher が落ちると reclaim / 再起動の規則（ADR-0003、`repository/restart.rs`）が invocation を `OutcomeUnknown` / `Failed{Host.Restarted}` にする。非同期 invocation にそのまま使うと、1 回の crash で retry の機会を失う。
- ADR-0010 の結果: 非同期 invocation の `dispatcher_id` は `None` で、guard（`repository/guard.rs::invocation_update`）はこれを不変とする。`queue_deadline` は記録するだけで強制していない。
- handler の外部副作用は platform の外にある。副作用の後、結果を台帳に書く前に落ちれば、その実行が起きたことを platform は知らない。exactly-once は約束できない。

## 選択肢

### 所有の表し方

| 案 | 内容 | 採否 |
|---|---|---|
| A | `invocations.dispatcher_id` を dispatch のたびに書き換えられるよう guard を緩める | 不採用。reclaim / 再起動の規則は `owner_id` の付いた非 terminal な invocation を terminal にする。dispatcher が死ぬたびに invocation が終わり、retry にならない |
| B | 別の `async_dispatch` 行に claim（owner、attempts、claim 期限、generation）を持ち、invocation の `dispatcher_id` は `None` のまま（guard は変えない）。reclaim / 再起動は非同期 invocation の **attempt だけ**を settle し、invocation は settle しない | **採用** |

### retry の予約

| 案 | 内容 | 採否 |
|---|---|---|
| A | message を NAK（delay 付き）して broker に再配送させる | 不採用。delay と配送回数は consumer の状態で、kill -9 で失われる（ADR-0008）。`max_age` / `max_deliver` を超えると event が消え、台帳に宙に浮いた invocation が残る |
| B | 次の試行を台帳の outbox に **次の generation の event**（`next_attempt_at` = 予定時刻）として同じトランザクションで書き、今の message は ACK する | **採用**。broker が落ちても予定は台帳に残り、publisher が期限に publish する。古い generation の message が再配送されても台帳の generation と合わないので ACK して捨てる |

### redrive の形

| 案 | 内容 | 採否 |
|---|---|---|
| A | dead-letter になった invocation を非 terminal に戻して再実行する | 不採用。terminal は final（guard）で、履歴を書き換える |
| B | **新しい非同期 invocation** を作り、redrive 記録（`redrives`）で dead letter・元の invocation と結ぶ | **採用**。元の invocation と試行履歴はそのまま残る |

## 決定

### 1. 台帳（migration 008）

- `async_dispatch`: invocation ごとに 1 行（最初の claim で作る）。`state`（`running` / `scheduled` / `done` / `dead`）、`generation`、`attempts`（`max_attempts` に数えた run）、`deferrals`（数えない先送り）、`claimed_by` / `claim_expires_at`、`next_attempt_at`、最初と最後の試行時刻、`last_error`。
- `dead_letters`: 理由（`non_retryable` / `attempts_exhausted` / `expired` / `function_deleted` / `revision_unavailable` / `poison`）、`status`（`open` / `redriven`）、試行の要約、最後のエラー、入力の digest / size / 置き場所。`origin_key`（`inv:<id>` / `msg:<message id>:<sequence>`）が unique で、同じ invocation・同じ poison message は 1 件にしかならない。
- `redrives`: 誰が（principal の subject）、いつ、なぜ、どの dead letter から、どの元 invocation を、どの Revision で新しいどの invocation にしたか。
- `outbox.generation`（既定 0）。message id は generation 0 が invocation id、n ≥ 1 が `<invocation id>.g<n>`。envelope にも `generation`（既定 0）。

入力は複製しない: dead letter は `invocation_inputs` / `object_refs` の既存の行を指すだけ。object GC は `open` の dead letter が参照する object を消さない（`sqlite/objects.rs::claim_for_collection`）。

### 2. 所有と reclaim（guard を緩めず、settle の規則を変える）

- 非同期 invocation の `dispatcher_id` は `None` のまま。所有は `async_dispatch` の claim だけ。
- `SlotStore::reclaim_expired` と store を開くときの restart reconcile は、`mode = async` の invocation を terminal にしない。その attempt だけを settle する（dispatch 済みなら `OutcomeUnknown`）。restart reconcile は `invocation_inputs` を持つ invocation を状態によらず対象から外す（ADR-0010 §6 は `accepted` / `queued` だけだった）。
- 実行に使う slot の lease・fencing（PLT-4631）は同期 invoke と同じ。claim は「次の run を誰がやるか」、slot lease は「その run がどの環境で走るか」を決める。

### 3. 1 件の配送の処理（batch = 1）

`AsyncDispatcher::handle`:

1. envelope を decode。できない、version が違う、message id と generation が合わない、routing tenant が違う、台帳の invocation の tenant / function / revision / digest と合わない → **poison**（§8）。
2. invocation が **terminal なら ACK するだけ**（重複配送、terminal の commit 後に ACK を失った message の再配送）。何も実行しない。
3. delivery の generation が台帳と違う → ACK（古い世代）。生きた claim がある → ACK（claim の持ち主と reaper が決める）。`scheduled` で期限前 → 残り時間で NAK（時計のずれ）。
4. 実行前の行き止まり: 期限（§4）を過ぎた → `expired`、`attempts ≥ max_attempts`（claim した run が結果を残さずに消えた後）→ `attempts_exhausted`、function 削除済み → `function_deleted`、固定 Revision が設定 cache で解決できない → `revision_unavailable`、policy 拒否・未知 tenant → `non_retryable`。設定 cache の期限切れ・未配信・停止中の cold start 制限は先送り（§4）。retry（`attempts ≥ 1`）なら retry budget を取り、無ければ次の window まで先送り。
5. **claim**（`claim_dispatch`: generation・claim・予定の CAS、`attempts + 1`）。負けたら ACK。
6. 入力を台帳（inline）か object store（台帳に記録した tenant scope）から読み、digest を検証。
7. `InvokeService::run_async` で**同期 invoke と同じ driver** を走らせる: admission、環境、attempt + lease（`SlotStore::acquire`）、Invoke、分類、後始末、usage。attempt 番号は run をまたいで通し番号。driver は invocation を terminal にせず、結果を sink に返す（attempt・lease・環境・usage は同期と同じく記録する）。run の間は claim を TTL の 1/3 ごとに延長する。
8. 結果から決め（§4）、**settle**（`settle_dispatch`、1 トランザクション）: invocation（terminal か `queued`）、dispatch 行、dead letter、次世代の outbox event。fence は claim（owner と attempts）。負けたら何も書かず ACK。
9. **commit の後にだけ ACK**。

### 4. retry policy と分類

`[async_dispatch]`（既定値）: `max_attempts = 3`、`max_event_age_seconds = 21600`、`backoff_initial_ms = 1000`、`backoff_max_ms = 300000`、`backoff_floor_ms = 100`、`retry_budget = 100` / `retry_budget_window_seconds = 60`、`[[async_dispatch.function]]` で function ごとに `max_attempts` / `max_event_age_seconds` を上書き。

- **期限**: `min(accepted_at + max_event_age, queue_deadline)`。これ以降は run を始めない。次の予定がこれを越えるなら、その場で `expired` の dead letter にする。ADR-0010 で記録だけだった `queue_deadline` はここで強制する。
- **backoff**: full jitter。`n` 回目の後の遅延は `[0, min(max, initial × 2^(n-1))]` の一様乱数（下限 `backoff_floor_ms`）。数えない先送りは `deferrals` の回数で同じ式。
- **retry budget**: function ごと・window ごとに retry を `retry_budget` 回まで（gateway process ごと）。超えた retry は失敗ではなく次の window へ先送りする（`Host.RetryBudgetExhausted`、数えない）。retry storm を「失敗する依存先を叩き続ける」から「決まった速さ」に変える。期限は効く。
- **分類**（`retry::classify`）:

| 結果 | 扱い |
|---|---|
| 成功 | `succeeded`、`done` |
| `Cancelled`（利用者の cancel） | `cancelled`、dead letter にしない |
| `Host.FunctionDeleted` | dead letter `function_deleted` |
| `Host.SecretBindingUnavailable`（認可）、`Host.ResponseTooLarge` / `Runtime.ResponseTooLarge`、`Host.UnsupportedArtifact`、`Host.InvokeTooLarge` / `Host.InvokeEncode`、`Host.InputCorrupt`、`Host.InvalidInput`、`Host.PolicyDenied`、`Host.UnknownTenant` | dead letter `non_retryable`（1 回で） |
| admission の拒否・待ち超過（`QueueTimeout` class、`Host.StartCircuitOpen`）、`Host.GatewayShutdown`、`Host.DispatcherFenced`、設定 cache の期限切れ等、`Host.InputUnavailable`、`Host.RetryBudgetExhausted`、usage journal の拒否（`Host.UsageJournalFull` / `Host.UsageJournalUnavailable`） | **先送り**（`attempts` に数えない、`deferrals + 1`） |
| それ以外（handler エラー `UserError`、`Crash`、`InitError`、`Timeout`、`OutcomeUnknown`、`Host.SlotReclaimed`、その他 platform） | retry（数える）。`attempts ≥ max_attempts` なら dead letter `attempts_exhausted` |

handler エラーを retry に含めるのは AWS Lambda の非同期 invoke と同じ既定で、利用者が error type ごとに選ぶ設定は後続（非対象）。

### 5. admission と非同期の class

非同期の run も同期 invoke と同じ admission（容量台帳、公平 queue、tenant quota、breaker）を通る。そのうえで:

- **並列の上限**: gateway は `workers`（既定 `[capacity] max_concurrency` の半分、最低 1）本の worker で 1 件ずつ取り出す。非同期が node の並列を使い切ることはなく、同期 invoke には常に残りがある。
- **待ちの短さ**: 非同期の ticket の期限は `admission_wait_ms`（既定 2 s）。容量が空かなければ run は失敗ではなく先送りになり、同期 invoke の前で長く並ばない。

優先度つきの queue（同じ待ち行列で同期を先に出す）は実装していない。

**試行ごとの事前 admission**: 各 run は admission の前に `InvokeService::admit_async_attempt` を通る。今は usage journal（PLT-4642、`UsageMeter::admit`）が記録できることを確かめ、できなければ何も起動せずに `Host.UsageJournalFull` / `Host.UsageJournalUnavailable` を返す。これは利用者のせいではない platform の状態なので、run は**数えない先送り**になる（期限は効く）。予算の予約（PLT-4643）など、試行を始める前の拒否はこの関数に足し、同じく数えない先送りとして扱う。各 run の attempt は同期 invoke と同じく `AttemptSettled` を出し、attempt 番号は run をまたいだ通し番号、`attempt_kind` は 1 が `first`、2 以降が `retry`。

### 6. dead letter

- dead letter にするのは、invocation を terminal（最後のエラーで `failed`、dispatch 済みで結果不明なら `outcome_unknown`）にするのと**同じトランザクション**。
- `GET /v1/functions/{id}/dead-letters`、`GET /v1/dead-letters/{id}`（`invoke` role、tenant 内だけ。他 tenant の function は空の一覧、他 tenant の dead letter は 404）。
- invocation の詳細（`GET /v1/invocations/{id}`）に `dispatch`（state、attempts、deferrals、generation、`next_attempt_at`、`last_error`、`dead_letter_id`、`redriven_from`）。
- DLQ 専用の stream は作らない。dead letter は台帳にあり、ACK / term 済みの message は queue に残らない。

### 7. redrive

`POST /v1/dead-letters/{id}:redrive`（`/redrive` も可）、本文 `{revision_id?, reason?}`:

- `invoke` **と** `redrive` の両 role が要る（`Role::Redrive`、token の `roles = ["redrive"]`）。無ければ 403。他 tenant の dead letter は 404（role の検査の後、存在を漏らさない）。
- `open` の dead letter だけ。`redriven` は 409、poison（invocation が無い）は 409。
- Revision は既定で元の固定 Revision。`revision_id` を明示したときだけ**同じ function の**別 Revision（設定 cache で呼び出し元の principal として解決。他 function・他 tenant の Revision は 404 / 400、削除済み function は 409）。
- 新しい invocation（`accepted`、`Idempotency-Key` なし、新しい `queue_deadline`）・その入力行（元と同じ inline の bytes か同じ object の参照 + `object_refs` の attach、tenant が違えば拒否）・outbox event（generation 0）・redrive 記録・dead letter の `redriven` を **1 トランザクション**で書く。broker には request の中で書かない（ADR-0010 と同じ）。outbox の上限（429 / 503）も受付と同じく効く。
- 新しい invocation の `dispatch.redriven_from` と dead letter の `redrives` が互いを指す。

### 8. poison

- decode できない / 台帳と合わない event は dead letter `poison`（invocation に結ばず、routing tenant・message id・sequence・payload の digest と size・理由だけ）を記録してから `term` する。再配送しない。記録に失敗したら NAK（あとでやり直す）。
- 他 tenant の invocation を指す偽の envelope も「この tenant の invocation は無い」として poison にし、その invocation には何も起きない。
- handler が panic・crash を繰り返す event は poison ではなく、`Crash` の retry として数えられ、`attempts_exhausted` に収束する。

### 9. reaper（台帳が真実）

`AsyncDispatcher::reap`（`reaper_interval_seconds` ごと）:

- claim の期限が切れた `running`（dispatcher が run の途中で消えた）: `attempts < max_attempts` なら次の generation で予約（数えた attempt はそのまま、エラー `Host.AsyncRunAbandoned`、`OutcomeUnknown`）、使い切っていれば `attempts_exhausted`。
- 期限（§4）を過ぎた非 terminal: `expired`（未 publish の outbox 行は消す）。
- 未 publish の event が無く、最後の publish（か予定時刻）から `stall_timeout_seconds` 経ち、consumer の backlog が 0: broker が event を失ったとみなし、次の generation を publish する。

どれも fence は「生きた claim が無く、generation が変わっていない」（`DispatchFence::Unclaimed`）。

### 10. at-least-once 契約

- **配送も実行も at-least-once**。ACK を失えば再配送され（§3-2 で実行しない）、run の途中で dispatcher が消えれば、その run が外部に何をしたかに関係なく次の run が実行される（§9）。
- 台帳が保証するのは「**1 invocation に terminal の記録は 1 つ**」「dead letter は 1 invocation に 1 件」だけ。handler の外部副作用は、副作用の後・settle の前に process が落ちれば**もう一度起きうる**。
- 利用者は業務の冪等キー（注文 id など）で副作用を 1 回にする: 一意制約付きの insert、条件付き書込み、処理済みキーの記録。`examples/idempotent-async` は `effects/<order_id>.json` を `create_new` で作り、2 回目の実行は `applied: false` を返す。`docs/api.md` §5.6.3。
- `Idempotency-Key`（ADR-0010 §3）は**受付**の重複を 1 invocation にまとめるもので、実行の重複は防がない。

### 11. failpoint

| 名前 | 地点 | 再起動後 |
|---|---|---|
| `dispatch.after_claim` | claim の後、handler の前 | claim 期限後に再配送（か reaper）で次の attempt |
| `dispatch.before_commit` | run の後（副作用は起きた）、settle の前 | 同上。handler はもう一度実行される |
| `dispatch.before_retry_commit` | retry を決めた後、予約の commit の前 | 同上 |
| `dispatch.after_commit` | settle の commit の後、ACK の前 | 再配送は terminal / 古い generation として ACK だけ |

## 結果（consequences）

- 実行は同期 invoke と同じ driver で、admission・隔離・usage・ログ・drain（PLT-4635）の規則がそのまま効く。
- 1 回の retry ごとに outbox → broker → consumer を 1 往復する。retry の遅延の下限は publisher の周期（既定 200 ms、settle で起こす）。
- `outbox` の未送信件数に予約中の retry が入る（`created_at` = 予定時刻なので滞留時間には数えない）。retry が多いと受付の `backlog` 上限（件数）に早く達する。
- retry budget は gateway process ごとで、複数 gateway の合計ではない。
- claim が切れた run の handler が遅れて終われば、その結果は fence で捨てられるが、handler は 2 回走っている（§10）。
- 非同期の run が invocation を `running` にした状態で gateway が落ちると、invocation は `running` のまま claim 期限と reaper を待つ（最大 `claim_ttl_seconds` + `reaper_interval_seconds`）。
- dead letter の inline 入力は台帳に残り続ける（削除・保持期限は未実装）。

## 検証

| 受入条件 | テスト / 記録 |
|---|---|
| terminal 保存後に ACK、ACK 喪失後の再配送は処理済み | `dispatch_tests::an_ack_lost_after_the_terminal_commit_is_not_run_again`、E2E `crash.after_commit.*` |
| 外部副作用 → DB 確定前の再試行（at-least-once） | `dispatch_tests::a_crash_between_the_side_effect_and_the_commit_runs_the_handler_again`、E2E `crash.before_commit.*`（2 回実行、効果 1 回） |
| consumer 停止 → 別 consumer が続行、fencing で二重 terminal なし | `dispatch_tests::a_consumer_that_stops_mid_run_is_taken_over_after_its_claim_expires`、`a_settle_from_a_claim_that_was_taken_over_is_refused`、`a_crash_before_the_retry_commit_is_retried_after_the_claim_expires` |
| 重複配送 | `dispatch_tests::concurrent_duplicate_deliveries_run_the_handler_once` |
| 入力 / 権限エラーを retry しない、期限・回数超過は DLQ | `a_non_retryable_error_is_dead_lettered_immediately`、`exhausted_attempts_are_dead_lettered_with_the_last_error`、`an_event_past_its_maximum_age_is_dead_lettered_without_running`、`a_function_deleted_during_retries_is_dead_lettered`、`retry::tests::classification_separates_retryable_deferrable_and_dead`、E2E `deadletter.*` |
| backoff / jitter / retry budget | `a_retryable_failure_is_retried_with_backoff_and_then_succeeds`、`retries_over_the_budget_are_deferred_without_counting`、`retry::tests::*` |
| poison | `poison_events_are_dead_lettered_once_and_never_redelivered`、E2E `poison.*` |
| redrive の認可・監査・Revision / 入力の保持・tenant 境界 | `redrive_is_authorized_and_never_crosses_a_tenant`、`a_redrive_creates_an_audited_invocation_on_the_pinned_revision_and_input`、gateway `dead_letters_and_redrive_are_authorized_audited_and_tenant_scoped`、E2E `redrive.*` |

## 非対象

- 利用者が error type ごとに retry するかを決める設定、Revision の spec に持つ retry policy（今は `[async_dispatch]` の function 単位の上書きだけ）。
- 非同期の結果の通知（callback / destination）、DLQ を外部 stream に流すこと。
- dead letter と inline 入力の保持期限・削除。
- 優先度つきの admission queue。retry budget の複数 gateway での共有。
- HTTP adapter 経由の非同期。

## 参照

- ADR-0003（台帳・restart 規則）、ADR-0006（admission）、ADR-0008（queue、JetStream の配送回数）、ADR-0009（drain・削除）、ADR-0010（受付と outbox）
- `docs/api.md` §5.6.2・§5.6.3、`docs/architecture.md` §4「非同期 dispatcher・retry・DLQ」、`docs/threat-model.md` T40〜T44・§14-16、`docs/cli.md` §「dead-letters」
