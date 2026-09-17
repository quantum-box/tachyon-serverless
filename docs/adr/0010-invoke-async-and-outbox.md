# ADR-0010: `invokeAsync` は入力・Invocation・outbox event を台帳の 1 トランザクションで確定してから 202 を返し、配送は outbox publisher が claim → publish → mark で行う（PLT-4639）

## ステータス

Accepted（2026-09-17、PLT-4639）。受付と配送（queue に載るまで）が範囲。queue から取り出して実行する dispatcher・retry・DLQ は PLT-4640。

実装:

- API: `POST /v1/functions/{id}:invokeAsync`（`/invokeAsync` も可）、`apps/gateway/src/handlers.rs::run_invoke_async`
- 受付: `crates/application/src/services/invoke_async/mod.rs`（`AsyncInvokeService::accept`）
- 台帳: `crates/application/src/repository/outbox.rs`（`AsyncInvocationRepository`）、`crates/application/src/repository/sqlite/outbox.rs`、migration `006_invoke_async_outbox.sql`
- publisher: `crates/application/src/services/invoke_async/publisher.rs`（`OutboxPublisher`）、gateway の loop は `apps/gateway/src/lib.rs::serve`
- 配送された event の読み戻し: `crates/application/src/services/invoke_async/consumer.rs`（`read_delivery`）
- failpoint: `crates/application/src/failpoints.rs`（feature `failpoints`、unit test では常に有効）
- 設定: `[invoke_async]`（`crates/application/src/services/invoke_async/config.rs`）
- E2E: `scripts/queue/async-e2e.sh`、証跡 `docs/evidence/async-e2e-*/`

## コンテキスト

- 非同期 invoke は「202 を返した後にプロセスが止まっても、受け付けた処理を失わない」ことが要る。202 を返した時点で client は再送をやめるので、受付の事実は 202 より前に永続化されていなければならない。
- ADR-0008 は queue を配送、台帳（`state.db`）を決定の正本とし、「object の put と invocation の insert の競合」は tombstone で安全にしたうえで、attach を invocation insert と同じトランザクションに入れるのは PLT-4639 とした。
- 同じ request を queue と台帳の両方に書く（dual write）と、片方だけ成功した状態が残る。queue に先に書けば台帳に無い event が配送され、台帳に先に書けば queue に載らない invocation が残る。どちらも「DB commit 前に 202 を返さず、broker への単純二重書込みで済ませない」という受入条件に反する。
- ADR-0008 の追記のとおり、JetStream の dedup 表と配送回数は kill -9 をまたいで保証されない。exactly-once を queue に頼れない。

## 選択肢

| 案 | 内容 | 採否 |
|---|---|---|
| A | transactional outbox: 台帳の 1 トランザクションで invocation と outbox 行を書き、commit 後に別 task が publish | **採用** |
| B | request の中で台帳に commit → 続けて broker に publish → 202 | 不採用。commit 後・publish 前に落ちると queue に載らない invocation が残り、それを拾う仕組み（= outbox）が結局要る。publish の待ちが受付の latency と可用性に直結する |
| C | broker に publish → 台帳に commit → 202 | 不採用。publish 後・commit 前に落ちると台帳に無い event が配送される |
| D | 台帳を queue として dispatcher が直接 poll（outbox から publish しない） | 不採用。ADR-0008 で queue を NATS JetStream にした決定（可視性 timeout、backlog、複数 consumer）を捨てることになる |

## 決定

### 1. 受付（`AsyncInvokeService::accept`）

1. 認可と解決は同期 invoke と同じく設定 cache から行う（`alias` / `revision_id` の query も同じ）。**revision はここで固定し、以後解決し直さない**。cold start の gate（PLT-4636）はこの時点では問わない（今は起動しない。起動時に dispatcher が問う）。削除中・削除済みの function は、同期 invoke と同じ経路（設定 cache の解決、PLT-4635）で 409 `function_deleted`（`error_type = Host.FunctionDeleted`）になり、何も保存しない。
2. 入力は JSON として解釈して直列化し直し（同期 invoke と同じ digest）、`limits.max_payload_bytes` を超えれば 413。
3. `Idempotency-Key` が既に結び付いていれば、何も保存せずに答える（§3）。
4. **早期の backlog 判定**（§4）。拒否ならここで返し、object を put しない。
5. `inline_input_max_bytes`（既定 64 KiB）以下の入力は台帳（`invocation_inputs.inline_body`）に置く。超えるものは object store（`[objects]`）に put し、平文 digest と size が一致することを確かめる。object store が無ければ 413 `input_too_large`。
6. **1 トランザクション**（`AsyncInvocationRepository::accept_async`、`BEGIN IMMEDIATE`）:
   1. key の live binding を読む（あれば `Existing` で何も書かない）
   2. outbox の未送信件数と最古の未送信の受付時刻を読み、上限を超えていれば `Backlog` で何も書かない（**同じ write lock の下**なので、同じ `state.db` の複数 gateway が同時に上限を越えることはない）
   3. invocation（`mode = async`、`status = accepted`、`revision_id` 固定、`input_digest` / `input_size_bytes`、`dispatcher_id` なし）
   4. idempotency binding
   5. object 入力なら `object_refs` への attach（tenant 一致と tombstone の検査を同じトランザクションで。ADR-0008 §5 の競合はここで閉じる）
   6. `invocation_inputs` 行（`inline` か `object` と `(object_id, region)`）
   7. outbox 行（`event_id = invocation id`、`payload` = routing envelope（§2）、`sent = 0`）
7. **COMMIT の後にだけ** 202 を返す。request の中で broker には何も送らない（publisher を起こすだけ）。

失敗時:

| 失敗 | 応答 | 残るもの |
|---|---|---|
| object put の後、トランザクションの前に落ちた / 失敗した | 500（failpoint）/ 接続断 | 参照の無い object。GC が `orphan_grace_seconds` の後に回収する（ADR-0008 §5）。台帳には何も無く、key も消費しない |
| トランザクション内（COMMIT 前）で台帳が失敗 | 503 `control_plane_unavailable`（`Host.StoreUnavailable`） | 同上（rollback） |
| COMMIT 後、202 の前に落ちた | 接続断 | 受付済み。client が同じ key で再送すれば同じ invocation の 202（`replayed = true`）。key なしの再送は別の invocation になる（同期 invoke と同じ） |
| object store が応答しない | 503 `async_unavailable`、`reason = object_store_unavailable` | なし |
| tenant の object quota 超過 | 429 `capacity_exceeded`、`reason = object_quota` | なし |
| object の size 上限超過・object store なしで inline 上限超過 | 413 `payload_too_large`、`reason = input_too_large` | なし |

**object を受付の中で消さない。** エラーが返ってもトランザクションが commit されたかどうかは台帳だけが知っている（commit の応答が失われる場合がある）。消してよいかは GC が台帳の参照を見て決める（非 terminal の invocation が参照していれば TTL に関係なく残す）。

### 2. outbox 行と envelope

- topic `invoke`、message id = event id = invocation id（`inv_<ULID>`、NATS header と SQL key に安全な文字だけ）。
- `payload` は routing envelope（`InvokeEnvelope`、version 1）: `invocation_id`、`tenant_id`、`function_id`、`revision_id`（受付時に固定）、`event_kind`、`input_digest`、`input_size_bytes`、`input_storage`、`accepted_at`、`queue_deadline`、`trace_id`。**入力本文は入れない**（queue に本文を複製しない。大きさは 1 KiB 未満で `max_message_bytes` に十分収まる）。
- 配送された event は envelope だけを信じない（`read_delivery`）: message id と envelope の id、routing tenant と envelope・台帳の tenant、revision・function・digest を台帳と照合し、入力は台帳の行（object なら台帳に記録した tenant scope）から読んで digest を検証する。一致しなければ 404 相当（別 tenant の存在を漏らさない）。

### 3. Idempotency-Key

- scope は同期 invoke と同じ `(tenant, function, key)` で、key 表も同じ（PLT-4631）。
- 同 key・同 input: 同じ invocation を 202（`replayed = true`、その時点の `status`）。受付前の読みで見つかれば object を put しない。トランザクション内で見つかった（並行 request に負けた）場合は、put 済みの object が orphan として GC に回る。
- 同 key・異なる input: 409 `conflict`（`Host.IdempotencyKeyReused`、`invocation_id` 付き）。
- **key は mode をまたがない**: 非同期の invocation に結び付いた key で同期 invoke を送る、またはその逆は 409 `conflict`。同期の replay は結果を待つが、非同期の invocation は client deadline を大きく越えて queue に居うるため。
- binding は invocation が terminal になるまで失効しない。

### 4. backlog admission と queue 停止時の方針

publisher は publish の結果から queue の状態（`Healthy` / `Full` / `Unavailable`）を覚える（プロセスごと）。判定（`backlog_refusal`）:

| outbox が上限（`max_pending_events` 件、または最古の未送信が `max_pending_age_seconds` より古い）を | queue の状態 | 応答 |
|---|---|---|
| 超えていない | Healthy / Unavailable | 受け付ける |
| 超えていない | Full（かつ未送信がある） | 429 `capacity_exceeded`、`reason = queue_full` |
| 超えた | Healthy | 429 `capacity_exceeded`、`reason = backlog` |
| 超えた | Unavailable | 503 `async_unavailable`、`reason = queue_unavailable` |
| 超えた | Full | 429 `reason = queue_full` |

**queue が止まっていても、outbox に余裕がある間は受け付ける。** 受付の耐久性は台帳が持っているので、broker の一時停止で受付まで止める理由がない。outbox は件数と滞留時間の両方で有界なので、停止が長引けば上限で 503 に切り替わり、台帳が際限なく膨らむことはない。429 と 503 を分けるのは、client が「混んでいる（待てば通る）」と「配送系が止まっている」を区別できるようにするため。

判定はトランザクションの前（object の put を避けるため）と、トランザクションの中（件数と age だけ、write lock の下で）の 2 回行う。queue の状態はプロセスローカルで、別 gateway の publish 失敗は知らない（件数と age の上限はどの gateway にも効く）。

### 5. publisher（`OutboxPublisher::run_once`）

1. **claim**: `sent = 0 AND next_attempt_at <= now AND (claimed_by IS NULL OR claim_expires_at <= now)` の行を最大 `publish_batch` 件、`claimed_by = <dispatcher id>`、`claim_expires_at = now + claim_ttl_seconds`、`publish_attempts + 1` にする。`BEGIN IMMEDIATE` の中で、UPDATE 自体も同じ述語の CAS なので、**同じ `state.db` の複数 gateway のうち 1 つだけが行を持つ**。
2. **publish**: message id = event id。
3. **mark**: broker の ACK の後、`sent = 1 ... WHERE sent = 0 AND claimed_by = <自分>`（CAS）。同じトランザクションで invocation を `accepted` → `queued`（それ以外の状態なら触らない）。自分の claim でなくなっていれば（期限切れ後に別 publisher が取った）何もしない（`lost_claims`）。
4. **失敗**: claim を外し、`next_attempt_at = now + min(retry_initial_ms * 2^(attempts-1), retry_max_ms)`、`last_error` を記録。`Unavailable` / `Unauthorized` なら同じ batch の残りも即座に外す（行ごとに timeout を待たない）。
5. 送信済みの行は `sent_retention_seconds`（既定 1 時間）後に削除する。
6. gateway は pass を `catch_unwind` で包んで loop する。受付があれば起こされ、無ければ `publish_interval_ms` 待つ。

crash の窓と収束:

| 窓 | 再起動後 |
|---|---|
| COMMIT 後、publish 前（claim 前） | 行は未送信のまま。次の pass が publish する |
| claim 後、publish 前 | claim が `claim_ttl_seconds` で切れ、次の pass が publish する |
| broker の ACK 後、mark 前 | claim が切れた後に再 publish。duplicate window（既定 120 s）内なら broker が `duplicate` と答えて何も保存しない（`duplicates`）。window の外、または JetStream の kill -9 で dedup 表が失われた場合（ADR-0008 §1）は、**同じ message id の 2 通目が保存される** |
| mark の後 | 何も起きない |

**配送は at-least-once。** 同じ invocation の event が 2 通届くことはあり、consumer（PLT-4640）は message ではなく台帳の invocation id で決着する（`queued` からの CAS、dispatcher lease）。queue の dedup は publisher の再送よけで、exactly-once の根拠にしない（ADR-0008 §1）。

### 6. 再起動で落とさない

- 台帳を開くときの restart reconcile（owner の無い非 terminal 行を `Failed{Host.Restarted}` にする規則、ADR-0003 / PLT-4631）は、**`invocation_inputs` を持つ `accepted` / `queued` の invocation を対象から外す**。非同期 invocation は dispatcher の持ち物ではなく、入力と event が永続化されているので、再起動後もそのまま配送を続ける。
- `dispatcher_id` は付けない（受け付けたプロセスが実行するとは限らない）。dispatcher の reclaim（lease 切れ）は owner の付いた行だけを見るので、これにも掛からない。

### 7. 非同期 invoke が有効になる条件

- `[queue]` が `none` 以外、かつ台帳が durable（`[store] backend = "sqlite"`）。揮発の台帳では受付の耐久性が無いので、`[queue]` があっても受け付けず 503 `async_unavailable`（`reason = not_configured`）。
- `[objects]` は任意。無ければ inline 上限を超える入力は 413。

### 8. failpoint

`crates/application/src/failpoints.rs`。名前付きの地点でエラー・panic・`SIGKILL` を起こす。**unit test か feature `failpoints` の build でだけ動く**（release の gateway は `TSLS_FAILPOINTS` を無視し、設定されていれば warn を出す）。application の instance ごとに持つ（並列 test が干渉しない）。

| 名前 | 地点 |
|---|---|
| `accept.after_object_put` | object put の後、トランザクションの前 |
| `accept.before_commit` | トランザクション内、全行を書いた後、COMMIT の前（rollback） |
| `accept.after_commit` | COMMIT の後、202 の前 |
| `objects.put_unavailable` | object store が応答しない |
| `outbox.before_publish` | claim の後、publish の前 |
| `outbox.queue_unavailable` | queue が応答しない |
| `outbox.after_publish` | broker の ACK の後、mark の前 |

## 結果（consequences）

- 受付の latency に broker の往復が入らない。代わりに、受付から queue に載るまでに publisher の周期（既定 200 ms、受付で起こされる）ぶんの遅れがある。
- 台帳の write lock を受付・claim・mark が共有する。SQLite の 1 file では write が直列化されるので、受付の throughput の上限はこれで決まる（未計測）。
- outbox の上限に達すると、queue が健全でも 429 になる（consumer が PLT-4640 まで無いので、E2E では publisher だけが outbox を減らす）。
- queue の状態がプロセスローカルなので、複数 gateway では `queue_full` / `queue_unavailable` を最初に観測するまでの遅れが gateway ごとにある。
- inline 入力の本文は invocation が terminal になっても台帳に残る（retention は未実装。PLT-4640 で terminal 化と同時に扱う）。
- invocation の `dispatcher_id` は不変（ADR-0003 の guard）で、非同期 invocation は `None` で作られる。PLT-4640 で dispatch 時の所有を表すには、別の lease 行か guard の緩和が要る。→ ADR-0013: guard は変えず、`async_dispatch` 行の claim で所有を表し、reclaim / restart は非同期 invocation の attempt だけを settle する。`queue_deadline` もそこで強制する。

## 検証

| 受入条件 | テスト / 記録 |
|---|---|
| DB commit 前に 202 を返さない、dual write しない | `services::invoke_async::tests::acceptance_commits_invocation_input_and_event_and_never_publishes_in_the_request`（受付直後に台帳に 3 行があり、queue は空）、`large_inputs_are_stored_as_objects_referenced_in_the_same_transaction` |
| commit 後 / publish 前の再起動で収束 | `accepted_but_unpublished_invocations_survive_a_restart_and_are_delivered`、`a_failure_after_commit_converges_on_the_same_invocation_after_a_restart`、`a_publisher_crash_loop_converges`、E2E `crash.before_publish.*`、`crash.after_commit.*` |
| publish 後 / 送信済み更新前の再起動で収束 | `a_crash_between_publish_and_mark_is_absorbed_by_the_broker_dedup`、`a_republish_outside_the_dedup_window_is_only_a_logical_duplicate`、E2E `crash.after_publish.*` |
| 複数 publisher で 1 回ずつ | `two_publishers_on_one_ledger_publish_each_event_once` |
| 同じ key は同じ Invocation、Revision は固定 | `the_same_idempotency_key_converges_on_one_invocation`、`a_key_bound_to_one_mode_is_a_conflict_in_the_other`、`the_revision_is_pinned_at_acceptance_across_alias_changes_and_republishes`、gateway `invoke_async_answers_202_with_a_status_url_and_converges_idempotently`、E2E `idempotency.*` |
| DB / object 停止・容量超過で正しく拒否、孤児 object を GC | `a_failure_before_commit_rolls_back_every_row_and_answers_503`、`a_failure_after_the_object_put_leaves_only_an_orphan_the_gc_collects`、`object_store_refusals_answer_with_their_reason_and_record_nothing`、`without_an_object_store_only_inline_inputs_are_accepted`、`an_outbox_over_its_bound_refuses_with_429_backlog`、`the_backlog_bound_holds_across_gateways`、E2E `gc.*` |
| queue 停止試験 | `a_queue_outage_fills_the_outbox_then_refuses_and_recovers`、E2E `outage.*`（nats-server を実際に停止・再起動） |
| tenant 越境 | `async_invocations_and_their_inputs_never_cross_a_tenant`、gateway `invoke_async_status_and_acceptance_never_cross_a_tenant` |

## 非対象

- queue からの取り出し、実行、retry、DLQ、`queue_deadline` の強制（PLT-4640）。
- 非同期の結果の取得（出力の object 化）、callback / destination。
- HTTP adapter 経由の非同期（`tachyon.http.v1`）。受け付けるのは JSON event だけ。
- 揮発の台帳（`[store] backend = "memory"`）での非同期 invoke。
- 複数 host での outbox（`state.db` は 1 host。ADR-0003）。

## 参照

- ADR-0003（台帳）、ADR-0006（admission）、ADR-0007（設定配信）、ADR-0008（queue と object）
- `docs/api.md` §5.6.1、`docs/architecture.md` §4「非同期 invoke と outbox（PLT-4639）」、`docs/threat-model.md`
