# 故障マトリクス: controller・DB・queue 障害時の復旧と重複計上防止（PLT-4646）

- 対象: `scripts/chaos/matrix.sh`（実行・集計）、`scripts/chaos/scenarios.sh`（シナリオ）、`scripts/chaos/lib.sh`（構成・workload・収束検査）
- 関連: [adr/0003](adr/0003-execution-state-persistence.md)（lease・fencing・restart reconcile）、[adr/0008](adr/0008-durable-queue-and-object-store.md)、[adr/0010](adr/0010-invoke-async-and-outbox.md)、[adr/0012](adr/0012-usage-ledger-and-rating.md)、[adr/0013](adr/0013-async-dispatch-retry-dlq.md)、[adr/0014](adr/0014-cron-and-webhook-triggers.md)、[acceptance.md](acceptance.md)「PLT-4646」
- 状態: macOS arm64 の 1 host・process provider で全 20 シナリオを 4 回実行した記録がある（§5、最終回は rebase 後の commit で 20 / 20 が初回試行で pass）。Firecracker では挙動が変わる 6 シナリオと guest OOM を KVM で実行した（§8、最終回 6 / 7、`stale_owner_sync_lease` は state.db の lock で失敗）。**2026-09-18**: その 2 つの不足を直し、シナリオを 1 つ追加（`stale_owner_frozen_in_transaction`、合計 21）して Firecracker 3 回・process 3 回を再実行した（§8.1、すべて pass）。

**ここにある結果は単一 host の結果であり、HA の保証ではない。** 2 つの gateway は同じ host・同じ `data_dir`（1 つの SQLite ファイル）を共有し、nats-server は 1 process・local disk・複製なし、object store は local の directory である。host そのもの・disk・network が壊れたときの挙動は何も示していない（§9）。

## 1. 目的

正常系のデモではなく、構成要素を止めて再開した後に状態が設計どおり収束することを、台帳そのものを読んで確かめる。

- durable に受け付けた Invocation が消えず、terminal を commit する前に queue の message を ACK しない。
- 同期は開始状況に応じて失敗（未 dispatch）か `OutcomeUnknown`（dispatch 済み）になり、非同期は policy どおり retry / DLQ に収束する。
- 古い owner（lease を失った gateway）の遅れた完了通知が新しい状態を上書きせず、UsageEvent の再送で二重計上しない。
- 復旧後に孤児の環境・Secret・object を安全に回収する。

## 2. 実行方法

```
scripts/chaos/matrix.sh                         # 全シナリオ。build してから実行（約 50 分）
scripts/chaos/matrix.sh --list                  # シナリオと故障の一覧
scripts/chaos/matrix.sh --only broker_sigkill,db_locked_within_lease --retries 0
TSLS_SKIP_BUILD=1 CHAOS_KEEP_WORK=1 scripts/chaos/matrix.sh --only baseline --evidence /tmp/chaos
```

- 必要なもの: `cargo`、`curl`、`jq`、`python3`（`sqlite3` module）、`perl`、`openssl`。nats-server は `scripts/queue/up.sh` が `deploy/nats/versions.env` の版を download して sha256 を照合する。docker・root・KVM は要らない。
- build: gateway は test 専用の `failpoints` feature 付き（release gateway では failpoint は無効）、`tachyon-queue-probe`、`tsls`、runtime bridge、`example-hello` / `example-idempotent-async` / `example-cpu-burn`。すべて debug build。
- 各シナリオは独立した subshell で、専用の scratch directory（`data_dir`、process provider の workdir、object root、usage journal）、空き port の nats-server、gateway process を持つ。前のシナリオの状態を持ち越さない。
- 失敗したシナリオは `--retries`（既定 2）回まで再実行する。**全試行を `results.jsonl` に残し**、2 回目以降で通ったものは summary で `FLAKY` と表示する。
- CI では実行しない（約 50 分・timing 依存のため）。`scripts/chaos/**` は shellcheck の対象。KVM は不要（`scripts/ci/classify-changes.sh` の KVM 必要 path に含めない）。

出力（既定 `docs/evidence/chaos-<UTC>/`）:

| file | 内容 |
|---|---|
| `results.jsonl` | 試行ごとに 1 行。`scenario`、`fault`、`attempt`、`injected_at`（故障を入れた時刻）、`restored_at`（故障を取り除いた時刻）、`recovered_at`（収束を観測した時刻）、`outage_ms`、`recovery_ms`（= recovered_at − restored_at）、`duration_ms`、`checks[]`（`name` / `ok` / `detail`）、`observations`（前後の件数・応答コード）、`result`（`pass` / `fail`）、`config_sha256` |
| `summary.md` | scenario / fault / outage_ms / recovery_ms / result / 試行 / 失敗した check |
| `profile.json` | commit、未 commit の file 数、OS、CPU 数、rustc、nats-server、bash、SQLite、provider、build、gateway binary と harness の sha256、実行したシナリオ、retries、seed（乱数 seed は使わない。secret 値と鍵はシナリオごとに乱数） |
| `scenarios/<id>/attempt-<n>/` | `checks.jsonl`、`observations.jsonl`、`result.json`、gateway log、gateway 設定（secret 値は `<redacted>`）、`executions.log`（副作用の実行記録）、`accepted.txt`（受付済み invocation）、nats-server log、シナリオ固有の記録 |
| `scenarios/<id>.attempt-<n>.log` | そのシナリオの進行 log |

`result` は、check が 1 つでも `ok: false`、故障を入れたのに収束を観測していない、またはシナリオが最後の step まで進まなかった（`harness.scenario_completed`）ときに `fail`。

## 3. 構成と workload

gateway（`scripts/chaos/lib.sh` の `gw_config`）: `profile = "dev"`、process provider、`[queue] backend = "nats"`（実 nats-server）、`[objects] backend = "filesystem"`（inline 上限 1024 byte、orphan grace 6 s、GC 2 s ごと）、usage journal と collector（300 ms ごと）、`[triggers]`（scheduler 200 ms）、dispatcher lease 6 s・heartbeat 1 s・clock skew 500 ms、outbox claim 3 s、async dispatch claim 4 s・ack wait 4 s・`max_attempts` 3・backoff 300〜1000 ms、secret binding 1 つ（値はシナリオごとの乱数）。シナリオが変える値はそのシナリオの節に書く。

workload（`wl_setup` / `wl_steady`）:

- 関数 3 つ: `chaos-async`（`examples/idempotent-async`、業務キー `order_id` で副作用を `effects/<order_id>.json` に 1 回だけ作り、実行を `executions.log` に記録。`sleep_ms` で副作用の前に待つ knob を PLT-4646 で追加）、`chaos-hello`（`examples/hello`、secret を `DEMO_SECRET` に bind）、`chaos-burn`（`examples/cpu-burn`、secret を bind）。
- 1 round: 同期 invoke（hello、`secret_present: true` を確認）、非同期 invoke 2 件（16 byte の inline 入力と 4096 byte の object 入力、`Idempotency-Key` 付き）、openssl で署名した webhook 1 件（hello）。cron trigger（hello、`*/2 * * * * *`、missed-run `skip`）が常に発火する。
- failpoint を使うシナリオと収束検査の前は cron を disable する（fire が failpoint に当たる・queue が空にならないため）。

収束検査（`cv_*`、すべての シナリオの最後に実行）。値はすべて gateway の API ではなく ground truth から読む:

| check | 読むもの | 条件 |
|---|---|---|
| `cv.accepted_never_lost` | `state.db` `invocations` | 202 を返した invocation の行がすべてある |
| `cv.accepted_all_terminal` / `cv.ledger_all_terminal` | `state.db` | 受付済みの非同期も含め、台帳の全 invocation が terminal |
| `cv.outcome_per_policy` | `state.db` | 受付済み非同期はすべて `succeeded`（シナリオが失敗を期待したものだけ `failed` / `outcome_unknown`） |
| `cv.one_outcome_per_invocation` | `dead_letters`、`invocations` | dead letter は invocation ごとに高々 1 件、`succeeded` かつ dead letter の invocation は無い。成功した attempt が複数ある invocation（commit 前 crash の再実行）は件数を observation に記録 |
| `cv.side_effect_exactly_once` | `effects/`、`executions.log` | 成功した注文ごとに副作用ファイルが 1 つ、`applied` の実行が 1 回 |
| `cv.outbox_drained` | `outbox` | 未送信 0 |
| `cv.jetstream_drained` | `tachyon-queue-probe stats` | consumer の `pending` と `ack_pending` が 0 |
| `cv.cron_each_time_once` | `trigger_fires` | 同じ予定時刻の fire が 2 行無い |
| `cv.usage_counted_once` | `usage/ledger.db`、`/readyz` | journal の未回収 0、ledger の event 数 = 異なる `event_id` 数、同じ `attempt_id` の `attempt_settled` が 2 件無い |
| `cv.graceful_stop_exit_0` | process | SIGTERM で exit 0 |
| `cv.no_orphan_processes` | process table | argv に scratch directory を含む bridge / user process が 0 |
| `cv.no_open_environments` | `environments` | 非 terminal の環境行 0 |
| `cv.no_secret_value_on_disk` | scratch directory 全体（gateway 設定を除く） | secret 値を含む file が 0（`state.db`、WAL、journal、ledger、workdir、log、object を含む） |

## 4. マトリクス

| # | scenario | 故障の入れ方 | 何を確かめるか（シナリオ固有の check） |
|---|---|---|---|
| 0 | `baseline` | なし | workload と収束検査そのものが通ること |
| 1 | `sync_gateway_kill` | 同時実行 1 の関数で 20 s の同期 invoke を実行中にし、2 件目を admission queue で待たせて gateway を SIGKILL、同じ `data_dir` で再起動 | client は両方とも応答なし（transport error）。dispatch 済みは `outcome_unknown` `Host.Restarted`、未 dispatch は `failed` `Host.Restarted`（platform error、attempt が走っていない）。worker process が残らない。未開始のものは新しい key で再実行でき、実行中だったものを同じ key で送ると記録済みの結果（502 `outcome_unknown`）を返して再実行しない |
| 2a | `async_kill_accept_after_commit` | failpoint `accept.after_commit=kill`（COMMIT 後・202 前） | 202 は返らないが行はある。client が同じ `Idempotency-Key` で再送すると `replayed: true`。1 回だけ実行 |
| 2b | `async_kill_after_publish` | `outbox.after_publish=kill`（broker の ACK 後・送信済みにする前） | crash 時点で台帳は非 terminal、未 ACK の message が 1。再起動後に再 publish（重複は JetStream が捨てる）、成功 1 回 |
| 2c | `async_kill_after_claim` | `dispatch.after_claim=kill`（claim 後・handler 前） | crash 時点で副作用 0・未 ACK 1。claim 期限後に再配送、成功 |
| 2d | `async_kill_before_commit` | `dispatch.before_commit=kill`（handler の副作用後・結果の commit 前） | crash 時点で `running`・副作用 1・未 ACK 1（terminal 前に ACK していない）。再起動後に handler が再実行され（at-least-once）、業務キーで 2 回目は `skipped`、副作用は 1 つ、terminal は 1 つ |
| 2e | `async_kill_after_commit_before_ack` | `dispatch.after_commit=kill`（terminal commit 後・ACK 前） | crash 時点で `succeeded` かつ未 ACK 1。再配送は ACK だけで処理され、実行 1 回・attempt 1 |
| 3a | `stale_owner_sync_lease` | 同じ `data_dir` の gateway A / B。A で 15 s の同期 invoke を実行中に A を SIGSTOP、B が lease を回収するのを待ち、A を SIGCONT | B は `lease_expires_at + skew` の後にだけ回収し（台帳の時刻で比較）、invocation は `outcome_unknown` `Host.LeaseExpired`。fence した環境を B が terminate して `Lost` に settle。SIGCONT 後の A の遅れた完了は台帳（invocation と attempt の行）を変えない。A は `/readyz` 503・`fenced: true`・新規 invoke 503、B は 200 を返し続ける。A の client には 502 `outcome_unknown`。A は再起動で復帰 |
| 3c | `stale_owner_frozen_in_transaction`（2026-09-18 追加） | 3a の KVM 最終回の失敗を決定的に再現する: A を `TSLS_STORE_FREEZE_FLAG` failpoint（failpoints build・dev profile）で **`state.db` の書込み transaction の中（`BEGIN IMMEDIATE` と `COMMIT` の間）で SIGSTOP** させ、両方の lease を過ぎるまで止めてから SIGCONT | A が transaction の中で止まったこと（process state `T`、止まった site の log）。止まっている間は誰も回収しない。SIGCONT 後に B が A を回収し B は回収されない、B は lease を取り戻した log を出し fence されない、A は stall を検出して fenced・新規 503、B は 200、fence された環境は terminate、invocation は 1 回だけ確定し（A の完了が回収より先に届けば `succeeded`、そうでなければ `outcome_unknown` `Host.LeaseExpired`）以後変わらない |
| 3b | `stale_owner_async_claim` | A だけで `sleep_ms: 9000` の非同期 invoke を実行中（attempt が dispatch 済み）にして SIGSTOP、B を起動、B が完了したら A を SIGCONT | B が claim 期限後に引き継いで完了（A の attempt は `outcome_unknown`、B の attempt が `succeeded`）。SIGCONT 後も invocation・attempt の行は不変、成功 attempt 1、dead letter 0、副作用 1 回。A は fenced |
| 4a | `db_locked_within_lease` | 別 process が `state.db` に `BEGIN EXCLUSIVE` を 20 s 保持（lease 60 s・heartbeat 5 s に変更）。保持中に管理 API の作成・同期 invoke・非同期受付を並行に送り続ける | 応答は「lock 解放後の成功」か「503 `Host.StoreUnavailable`」だけ（500・無応答なし）、503 が少なくとも 1 件。解放後は再起動なしで `/readyz` 200・invoke 200。作成できなかった関数の行・拒否された非同期 key の行が無い。cron が再開 |
| 4b | `db_locked_past_lease` | 同じ lock を 12 s（lease 6 s + skew を超える） | 応答は成功・503 `Host.StoreUnavailable`・fenced の 503（`provider_unavailable`）のいずれか。2026-09-17 の記録では **gateway は heartbeat の延長を拒否されて自分を fence し、再起動するまで 503 のまま**だった（`recovery.fenced_itself_after_losing_its_lease`）。2026-09-18 以降は、延長が store に届かなかっただけで誰にも回収されていない lease を lock の解放後に取り戻し、**再起動なしで 200 に戻る**（`recovery.serves_without_restart`、ADR-0003「store が止まった間の lease」、§6.2） |
| 5a | `broker_sigstop` | nats-server を SIGSTOP（応答しない broker）、`max_pending_events = 12` | 受付は outbox の上限まで 202、その後 503 `queue_unavailable`。同期 invoke は影響なし。SIGCONT 後に outbox が空になり全件 terminal |
| 5b | `broker_sigkill` | 非同期 handler 実行中（3 s）に nats-server を SIGKILL、停止中も object 入力の非同期を受け付け、同じ store で再起動 | 停止中も outbox に受付。再起動後に全件 terminal、副作用 1 回ずつ、consumer の未 ACK 0 |
| 6 | `object_store_unavailable` | まず `accept.after_object_put=kill` で孤児 object を作る。broker を止めて object 入力の非同期を outbox に保持し、object root を `chmod 000`、broker を再開 | 停止中の object 入力は 503 `object_store_unavailable`、inline は 202。保持していた invocation の実行は object を読めず成功しない（数えない先送り）、gateway は生きている。復旧後に成功し、GC が孤児を回収し、非 terminal の invocation の object は残る |
| 7 | `usage_journal_full_and_replay` | `journal_max_events = 60`・headroom 10・collector 停止（1 時間間隔）で同期 invoke を満杯まで。SIGKILL。collector を有効にして `TSLS_USAGE_CRASH_POINT=collector.after_ledger_commit`（batch 25）で起動（ledger commit 後・cursor 前に SIGKILL）。再起動 | 満杯で 503 `Host.UsageJournalFull`、拒否は invocation を作らない。非同期受付は 202 だが run は数えない先送りで実行しない。journal に event が残り ledger 0。replay 後 ledger の event 数 = journal の件数・重複は無視・`GET /v1/usage` の invocation / attempt 数 = 既知の成功 invoke 数。再び 200 |
| 8a | `worker_bridge_kill_sync` | 12 s の同期 invoke 実行中に runtime bridge を SIGKILL | client 502 `outcome_unknown`、台帳 `outcome_unknown`。環境は terminate 済み、次の invoke は 200 |
| 8b | `worker_user_process_kill_sync` | 同じく user process を SIGKILL | client 502 `crash`、台帳 `failed` `Runtime.Crash`。環境の後始末 |
| 8c | `worker_bridge_kill_async` | `sleep_ms: 6000` の非同期 run 中に bridge を SIGKILL | retry で成功（attempt 2 以上）、副作用 1 回、環境の後始末 |
| 9 | `control_plane_outage` | 既存の `scripts/control-plane/outage-e2e.sh`（管理 gateway を config TTL と認可 lease を超えて停止し、再起動） | 既存 E2E の exit 0 と `summary.json` の `ok`。時刻は step の所要時間から導いた概算 |
| 10 | `orphan_recovery_after_crash` | 孤児 object（orphan grace 45 s）、secret を bind した 20 s の同期 invoke 2 件（環境 2）、broker 停止中の object 入力の非同期 3 件（未送信 outbox）、scheduler lease を持った状態で gateway を SIGKILL、broker 再開、再起動 | crash 前後の件数を observation に記録。旧環境の process 0・旧環境の行が terminal・未送信 outbox 0・scheduler lease の owner が新しい dispatcher・参照の無い object 0・secret 値を含む file 0（前後とも） |

## 5. 結果

全シナリオを 4 回実行し、4 回とも evidence に残した。4 回目は origin/main（PLT-4618 TiDB・PLT-4643 予算・PLT-4644 console の統合後）に rebase した commit で、未 commit の変更なしで実行した。環境はすべて同じ（`profile.json`: Apple silicon の macOS 26.6.2 / Darwin 25.6.0 arm64、10 CPU、rustc 1.95.0、nats-server v2.14.7、SQLite 3.51.0、process provider、debug build + `failpoints`）。1 回目と 2 回目の失敗は harness の判定と sample の log の誤りで、直してから次の回を実行した（§5.4）。**flaky や失敗を消した回は無い。**

`outage_ms` は故障を入れてから取り除くまで、`recovery_ms` は取り除いてから収束を観測するまで（ms）。`recovery_ms` はシナリオの設定値に強く依存する: `async_kill_*` の 4〜5 s は outbox claim（3 s）と async claim（4 s）/ ack wait（4 s）の期限、`stale_owner_*` は lease 6 s + heartbeat と fence した gateway の再起動、`db_locked_past_lease` の約 30 s は「再起動なしで戻るか」を 30 s 待ってから再起動した時間、`broker_sigkill` は JetStream の再配送（ack wait）、`orphan_recovery_after_crash` の約 36 s はほぼ orphan grace（45 s、object 作成時点から）で決まる。**性能値や SLO ではない。**

### 5.0 4 回目（最終、`docs/evidence/chaos-20260917T115316Z/`、origin/main に rebase した commit `199b0d2`、未 commit 0）

20 / 20 が 1 回目の試行で pass。

| scenario | outage_ms | recovery_ms | 結果 | 試行 | 失敗した check（失敗した試行） |
|---|---:|---:|---|---|---|
| `baseline` | - | - | pass | pass |  |
| `sync_gateway_kill` | 70 | 366 | pass | pass |  |
| `async_kill_accept_after_commit` | 120 | 696 | pass | pass |  |
| `async_kill_after_publish` | 258 | 4502 | pass | pass |  |
| `async_kill_after_claim` | 216 | 4519 | pass | pass |  |
| `async_kill_before_commit` | 1113 | 3819 | pass | pass |  |
| `async_kill_after_commit_before_ack` | 850 | 327 | pass | pass |  |
| `stale_owner_sync_lease` | 9597 | 7220 | pass | pass |  |
| `stale_owner_async_claim` | 33903 | 7819 | pass | pass |  |
| `db_locked_within_lease` | 20298 | 203 | pass | pass |  |
| `db_locked_past_lease` | 12282 | 30051 | pass | pass |  |
| `broker_sigstop` | 8370 | 3450 | pass | pass |  |
| `broker_sigkill` | 8966 | 12278 | pass | pass |  |
| `object_store_unavailable` | 6352 | 1296 | pass | pass |  |
| `usage_journal_full_and_replay` | 883 | 479 | pass | pass |  |
| `worker_bridge_kill_sync` | 18 | 237 | pass | pass |  |
| `worker_user_process_kill_sync` | 15 | 174 | pass | pass |  |
| `worker_bridge_kill_async` | 14 | 6717 | pass | pass |  |
| `control_plane_outage` | 13616 | 494 | pass | pass |  |
| `orphan_recovery_after_crash` | 90 | 36098 | pass | pass |  |

4 回目の `stale_owner_sync_lease` は A の lease 期限の 695 ms 後（skew 500 ms）・SIGSTOP の 5923 ms 後に回収した。`db_locked_past_lease` の同期 invoke は lock 解放後に 200（3 回目は fence 後の 503）で、どちらも許容される応答。その他の観測値は 3 回目とほぼ同じ（下）。

### 5.1 3 回目（`docs/evidence/chaos-20260917T113111Z/`、squash 前の作業 commit `c3e659e`）

20 / 20 が 1 回目の試行で pass。

| scenario | outage_ms | recovery_ms | 結果 | 試行 | 失敗した check（失敗した試行） |
|---|---:|---:|---|---|---|
| `baseline` | - | - | pass | pass |  |
| `sync_gateway_kill` | 81 | 363 | pass | pass |  |
| `async_kill_accept_after_commit` | 128 | 727 | pass | pass |  |
| `async_kill_after_publish` | 362 | 4446 | pass | pass |  |
| `async_kill_after_claim` | 321 | 4626 | pass | pass |  |
| `async_kill_before_commit` | 1033 | 4092 | pass | pass |  |
| `async_kill_after_commit_before_ack` | 1128 | 422 | pass | pass |  |
| `stale_owner_sync_lease` | 9785 | 5839 | pass | pass |  |
| `stale_owner_async_claim` | 22325 | 7831 | pass | pass |  |
| `db_locked_within_lease` | 20177 | 200 | pass | pass |  |
| `db_locked_past_lease` | 12048 | 30579 | pass | pass |  |
| `broker_sigstop` | 11472 | 6282 | pass | pass |  |
| `broker_sigkill` | 9160 | 15372 | pass | pass |  |
| `object_store_unavailable` | 6592 | 1107 | pass | pass |  |
| `usage_journal_full_and_replay` | 856 | 1058 | pass | pass |  |
| `worker_bridge_kill_sync` | 103 | 282 | pass | pass |  |
| `worker_user_process_kill_sync` | 16 | 194 | pass | pass |  |
| `worker_bridge_kill_async` | 13 | 6430 | pass | pass |  |
| `control_plane_outage` | 13842 | 644 | pass | pass |  |
| `orphan_recovery_after_crash` | 105 | 36668 | pass | pass |  |

3 回目の主な観測値（`results.jsonl` の `observations` と `checks[].detail`）:

- `stale_owner_sync_lease`: A の lease 期限 11:33:03.214、B の回収 11:33:04.326（期限の 1111 ms 後、skew 500 ms）、SIGSTOP から 6577 ms。A の client は 502 `outcome_unknown` / `Host.LeaseExpired`。環境は `lost`・process 0。
- `stale_owner_async_claim`: B の引き継ぎは SIGSTOP から 22 s（claim 期限・ack wait・lease の回収を含む）。A の log に `stale completion refused` と `the claim was taken over while running; this run's outcome will be refused`。
- `db_locked_within_lease`: 20 s の lock 中の 3 要求は、管理 API 503 `Host.StoreUnavailable`、同期 invoke 200・非同期 202（lock 解放後に完了、最長 19.9 s）。
- `db_locked_past_lease`: 管理 API 503 `Host.StoreUnavailable`、同期 invoke 503 `provider_unavailable`（fence 済みの dispatcher）、非同期 202。解放後も `/readyz` 503 `fenced: true` のまま、再起動で復帰。
- `broker_sigstop`: 停止中 30 件の受付のうち 10 件 202（outbox 上限 12 に cron の fire を含む）、20 件 503 `queue_unavailable`、同期 3 / 3 が 200。
- `object_store_unavailable`: 保持していた object 入力の invocation は停止中に実行されず、復旧後に attempt 1 で成功（数えない先送り 17 回）。孤児 object 1 → 0。
- `usage_journal_full_and_replay`: 成功 10 件の後 503 `Host.UsageJournalFull`、journal 50 件・ledger 0 → collector crash で ledger 25・cursor 0 → replay 後 ledger = journal の回収数・重複 25 を無視・`GET /v1/usage` は invocation 10 / attempt 10。
- `orphan_recovery_after_crash`: crash 前 worker process 2・open 環境 2・未送信 outbox 5・参照の無い object 1・secret を含む file 0 → crash 直後の worker process 0（§6.2）→ 復旧後すべて 0、scheduler lease の owner は新しい dispatcher。

### 5.2 2 回目（`docs/evidence/chaos-20260917T110929Z/`、squash 前の作業 commit `95f4a80`）

| scenario | outage_ms | recovery_ms | 結果 | 試行 | 失敗した check（失敗した試行） |
|---|---:|---:|---|---|---|
| `baseline` | - | - | pass | pass |  |
| `sync_gateway_kill` | 264 | 610 | pass | pass |  |
| `async_kill_accept_after_commit` | 406 | 1177 | pass | pass |  |
| `async_kill_after_publish` | 1781 | 4763 | pass | pass |  |
| `async_kill_after_claim` | 850 | 4389 | pass | pass |  |
| `async_kill_before_commit` | 1653 | 3520 | pass | pass |  |
| `async_kill_after_commit_before_ack` | 1390 | 453 | pass | pass |  |
| `stale_owner_sync_lease` | 9337 | 7048 | pass | pass |  |
| `stale_owner_async_claim` | 22835 | 7205 | pass | pass |  |
| `db_locked_within_lease` | 20314 | 400 | pass | pass |  |
| `db_locked_past_lease` | 11910 | 30229 | FLAKY（2 回目で pass） | fail → pass | cv.side_effect_exactly_once |
| `broker_sigstop` | 7156 | 3347 | pass | pass |  |
| `broker_sigkill` | 8749 | 12224 | pass | pass |  |
| `object_store_unavailable` | 6274 | 1617 | FLAKY（2 回目で pass） | fail → pass | cv.cron_each_time_once |
| `usage_journal_full_and_replay` | 687 | 457 | FLAKY（3 回目で pass） | fail → fail → pass | replay.ledger_counts_each_event_once |
| `worker_bridge_kill_sync` | 15 | 221 | pass | pass |  |
| `worker_user_process_kill_sync` | 16 | 204 | pass | pass |  |
| `worker_bridge_kill_async` | 7 | 6695 | FLAKY（2 回目で pass） | fail → pass | cv.cron_each_time_once |
| `control_plane_outage` | 13286 | 570 | pass | pass |  |
| `orphan_recovery_after_crash` | 77 | 36490 | pass | pass |  |

### 5.3 1 回目（`docs/evidence/chaos-20260917T105520Z/`、squash 前の作業 commit `d992d95`）

| scenario | outage_ms | recovery_ms | 結果 | 試行 | 失敗した check（失敗した試行） |
|---|---:|---:|---|---|---|
| `baseline` | - | - | pass | pass |  |
| `sync_gateway_kill` | 182 | 534 | pass | pass |  |
| `async_kill_accept_after_commit` | 521 | 1524 | pass | pass |  |
| `async_kill_after_publish` | 772 | 4561 | pass | pass |  |
| `async_kill_after_claim` | 798 | 4809 | pass | pass |  |
| `async_kill_before_commit` | 2007 | 4383 | pass | pass |  |
| `async_kill_after_commit_before_ack` | 1681 | 452 | pass | pass |  |
| `stale_owner_sync_lease` | 10916 | 6613 | FLAKY（2 回目で pass） | fail → pass | reclaim.not_before_lease_expiry |
| `stale_owner_async_claim` | 23957 | 7576 | pass | pass |  |
| `db_locked_within_lease` | 20555 | 851 | pass | pass |  |
| `db_locked_past_lease` | 12199 | 29985 | FAIL | fail → fail → fail | outage.answers_success_or_503_store_unavailable |
| `broker_sigstop` | 8738 | 3480 | pass | pass |  |
| `broker_sigkill` | 9426 | 10528 | pass | pass |  |
| `object_store_unavailable` | 6622 | 1092 | pass | pass |  |
| `usage_journal_full_and_replay` | 1164 | 590 | pass | pass |  |
| `worker_bridge_kill_sync` | 38 | 493 | pass | pass |  |
| `worker_user_process_kill_sync` | 28 | 402 | pass | pass |  |
| `worker_bridge_kill_async` | 16 | 6678 | pass | pass |  |
| `control_plane_outage` | 13979 | 2140 | pass | pass |  |
| `orphan_recovery_after_crash` | 376 | 32550 | pass | pass |  |

### 5.4 1 回目・2 回目の失敗の原因

どれも台帳・queue・usage の不変条件が破れたものではなかった。原因を確かめてから直し、次の回で全シナリオを再実行した。

| 回 | scenario | check | 原因 | 対処 |
|---|---|---|---|---|
| 1 | `stale_owner_sync_lease` | `reclaim.not_before_lease_expiry` | 判定の誤り。「SIGSTOP から 6 s 以上たってから回収」としていたが、lease は最後の heartbeat（SIGSTOP の最大 1 s 前）から数える。5939 ms での回収は正しい | 台帳の `dispatchers.lease_expires_at` と `reclaimed_at` を比べる（`reclaim.not_before_lease_expiry_plus_skew`） |
| 1 | `db_locked_past_lease`（3 回とも） | `outage.answers_success_or_503_store_unavailable` | 判定の誤り。lease を失って fence した dispatcher の同期 invoke の 503（`provider_unavailable`、`error_type` なし）を「その他の応答」と数えた | fence の 503 も許容（`outage.answers_success_or_retryable_503`） |
| 2 | `db_locked_past_lease` | `cv.side_effect_exactly_once` | **`examples/idempotent-async` の実行 log の欠陥**。`writeln!` が 1 行を複数の write(2) で書き、同時に走った 2 つの run の行が混ざった（`applied` の直後に別の行が続き、grep が 1 件を数え損ねた）。副作用ファイルは正しく 1 つ | 1 行を 1 回の `write_all` で書く |
| 2 | `object_store_unavailable`、`worker_bridge_kill_async` | `cv.cron_each_time_once` | 判定の誤り。cron を最初の発火の前に disable したシナリオで fire 0 件を失敗にしていた | 「同じ予定時刻が 2 行無い」だけを判定（0 件は許容） |
| 2 | `usage_journal_full_and_replay`（2 回） | `replay.ledger_counts_each_event_once` | 判定の誤り。crash 前に数えた journal の件数（50）と比べていたが、再起動した gateway も event を追記する（51） | replay 後の journal の cursor（回収した全件）と ledger の件数・distinct を比べる |

### 5.5 開発中の部分実行

harness を作る途中の部分実行（scratchpad、commit 前、evidence に含めない）で §6.1 の 2 つの実装の問題を見つけた。

## 6. 見つかった問題

### 6.1 修正したもの

| 問題 | 症状 | 修正 | 回帰テスト |
|---|---|---|---|
| kill failpoint が SIGABRT で死ぬ | macOS で `outbox.after_publish` / `dispatch.after_claim` の failpoint が exit 134（SIGABRT）になった。`kill(getpid(), SIGKILL)` は他の thread が走っていると配送前に戻り、直後の `abort()` が先に届く。crash の意味（後続を実行しない）は同じだが、「SIGKILL で死ぬ」という failpoint の約束と違った | `failpoints.rs::kill_self`: SIGKILL の後は `abort()` せず、配送を待つ（10 s の安全網の後にだけ abort）。usage の crash point は既にこの形だった | `crates/application/src/failpoints.rs::tests::a_kill_failpoint_dies_of_sigkill_even_with_busy_threads`（busy thread 8 本の子 process を 10 回起動し、全部 signal 9。修正前は 1 回目で signal 6） |
| store が lock されると要求が lock の間ずっと待つ | 別 process が `state.db` の書込み lock を持つと、管理 API・invoke は 503 を返さず lock が外れるまで待った（20 s の lock で 21 s）。`SqliteStore` は 1 本の connection を mutex で直列化しており、mutex を持った呼び出しが SQLite の `busy_timeout`（5 s）を待つ間、他の呼び出し（heartbeat、outbox publisher、scheduler、GC、HTTP）が mutex の列に並ぶ。n 番目の呼び出しは n × 5 s 待つ | `repository/sqlite/mod.rs`: mutex の取得も `STORE_WAIT`（5 s）で打ち切り、`RepoError::Store`（503 `Host.StoreUnavailable`）を返す。1 回の store 呼び出しは最長 約 10 s | `crates/application/src/repository/sqlite/tests.rs::a_held_write_lock_fails_every_concurrent_caller_within_a_bounded_time`（`BEGIN EXCLUSIVE` を保持したまま 4 thread が同時に書く → 全員 `Store` エラー、各 ≤ 12 s。修正前は 16 s 待った） |
| 他の SQLite store も同じ mutex の形で待つ | usage journal（`usage/journal.db`）、usage ledger（`usage/ledger.db`）、budget store（`usage/budget.db`）、SQLite queue（`queue.db`）も 1 本の connection を mutex で直列化し、mutex の取得に上限が無かった。file が別 process に lock されると n 番目の呼び出しは n × busy timeout 待つ。budget store と usage journal は記録用の状態（`last_error`、unjournaled の数）も同じ mutex の中にあり、`/readyz` まで lock の間待った | 共通の `crates/application/src/sqlite_wait.rs`（`STORE_WAIT` 5 s、`lock_connection`）に寄せ、`repository/sqlite` もそれを使う。connection の取得を `STORE_WAIT` で打ち切り、各 store の「使えない」拒否にする: journal → `usage_journal_unavailable`（受付は fail closed、refusal は数える）、ledger → collector の失敗（cursor は動かず次の tick で再送、`/readyz` の `collector.last_error`）、budget store → `Host.BudgetStoreUnavailable`、SQLite queue → `QueueError::Unavailable`（busy timeout 切れの `SQLITE_BUSY` も同じ。outbox は行を保持し、dispatcher は先送り）。journal と budget store は記録用の状態を connection と別の mutex に分けた。1 回の呼び出しは最長 `STORE_WAIT` + busy timeout（journal / ledger / queue 約 10 s、budget 約 15 s） | `crates/application/src/usage/tests.rs::a_locked_journal_refuses_every_concurrent_append_within_a_bounded_time`（5.4 s。修正前は 21.5 s 待った）、`usage/tests.rs::a_locked_ledger_refuses_every_concurrent_delivery_within_a_bounded_time`（配送 4 件が各 ≤ 12 s で失敗、collect も失敗して cursor は動かず、解除後に全件配送。10.7 s。修正前は 1 件が 16.1 s）、`budget/tests.rs::a_locked_store_refuses_every_concurrent_reservation_within_a_bounded_time`（予約 4 件が各 ≤ 17 s で失敗、`last_error` は 1 s 未満で返る。10.7 s。修正前は `last_error` が待たされ 42.6 s）、`durable/sqlite_queue.rs::tests::a_locked_queue_refuses_every_concurrent_publish_within_a_bounded_time`（publish 4 件が各 ≤ 12 s で `Unavailable`。5.4 s。修正前は 21.4 s）。いずれも `BEGIN EXCLUSIVE` を別 connection で保持し、解除後は成功する |
| sample の実行 log の行が混ざる | `examples/idempotent-async` の `executions.log` で、同時に走った 2 つの run（at-least-once の再実行）の行が混ざり、実行回数の数え方を誤らせた（2 回目の `db_locked_past_lease`）。副作用そのもの（`create_new`）は正しい | 1 行を 1 回の `write_all` で書く | 3・4 回目の全シナリオの `cv.side_effect_exactly_once`（unit test は無い。sample の log 形式の修正） |

### 6.2 既知の制約・残課題（修正していない）

| 項目 | 内容 | 影響と扱い |
|---|---|---|
| ~~store 停止が lease を超えると gateway は自分を fence する~~ | **2026-09-18 に変更**: heartbeat の延長が store に届かなかった（他 process の書込み lock、connection の待ち上限）間に lease が期限を過ぎても、誰にも回収されておらず、その間 process 自身が止まっていなかった（stall watchdog）なら、store が答えた最初の延長で lease と未 release の slot lease を取り戻す（`SlotStore::renew_after_store_outage`、ADR-0003「store が止まった間の lease」）。`db_locked_past_lease` は再起動なしで復帰する | 回収との競合は store の transaction が直列化する（先に commit した方が勝つ）。process 自身が止まっていた（SIGSTOP・VM 停止・5 s 以上の starvation）場合は従来どおり fence し再起動が要る |
| transaction の中で止まった process は lock を持ち続ける（2026-09-18 追加） | 1 つの `state.db` を共有する gateway の 1 つが書込み transaction の中で止まると（KVM 最終回の `stale_owner_sync_lease`、3c）、SQLite の lock は OS の file lock なので止まっている間は誰も書けない。他の gateway の書込みは 1 回の store 呼び出しあたり約 10 s で 503、heartbeat は失敗しても fence しない、回収はできない。再開後は止まっていた側が fence、動いていた側が lease を取り戻して回収する | lock を持つ時間を短くした（`stamp_config` の parse・hash・budget file を lock の外へ、変化が無ければ書込み transaction を開かない）ので当たる確率は下がるが、ゼロにはならない。止まったまま戻らない process は運用が kill する（kill すれば OS が lock を解放する）。250 ms 以上 lock を持った transaction は site 付きで log に残る（`SqliteStore::write`）。TiDB adapter では変わる |
| store lock 中の要求の総遅延 | 1 回の store 呼び出しは ~10 s で打ち切るが、同期 invoke は store を複数回呼ぶので、途中の呼び出しが成功すると全体では lock の長さまで遅れることがある（20 s の lock で 19.9〜20.3 s） | 503 か成功のどちらかで返ることは確認済み。request 全体の deadline は無い |
| ~~他の SQLite も同じ mutex の形~~ | **修正済み**（§6.1「他の SQLite store も同じ mutex の形で待つ」）。usage journal・usage ledger・budget store・SQLite queue の connection の取得を `STORE_WAIT` で打ち切った | 回帰テスト: `usage::tests::a_locked_journal_refuses_every_concurrent_append_within_a_bounded_time`、`usage::tests::a_locked_ledger_refuses_every_concurrent_delivery_within_a_bounded_time`、`budget::tests::a_locked_store_refuses_every_concurrent_reservation_within_a_bounded_time`、`durable::sqlite_queue::tests::a_locked_queue_refuses_every_concurrent_publish_within_a_bounded_time`（security regression group の resource_limits）。chaos matrix（`scripts/chaos/`）でこれらの file を lock するシナリオは無い |
| 非同期の遅れた settle の log | A（fence 済み）の遅れた完了は正しく拒否されるが、続けて「cannot requeue invocation ...: already in a terminal state」の platform error を log に出し、message を再配送させる | 台帳は変わらない（check 済み）。log が紛らわしいだけ |
| 数えない先送りに上限が無い | object store の読めない run と usage journal 満杯の run は `deferrals` を増やすだけで `max_attempts` に数えない（object store の停止 6.6 s で 11〜17 回） | `max_event_age_seconds`（既定 6 h）で最終的に `expired` の dead letter になる。長い停止での件数・負荷は測っていない |
| process provider の worker は gateway と一緒に止まる | gateway を SIGKILL すると bridge は host との接続を失って自分と user process を止める（crash 直後の worker process 0）。reconcile は台帳の環境を fence → `Lost` に settle するが、「生き残った process を terminate する」経路は process provider では実際には通らない | Firecracker では VMM が gateway と独立に残るので経路が違う（§8） |
| 同期の client への応答 | gateway の SIGKILL では client は transport error（HTTP 応答なし）。未開始（`failed` platform error）と結果不明（`outcome_unknown`）の区別は status URL（`GET /v1/invocations/{id}`）か同じ key の再送でしか分からない。invocation id は client に届いていないので、Idempotency-Key を付けない client は区別できない | `docs/api.md` §5.6（Idempotency-Key、再実行しない規則）どおり。client は key を付けて再送する |

## 7. 受入条件との対応

| 受入条件 | 状態（単一 host・process provider） | シナリオ |
|---|---|---|
| durable 受付済み Invocation が消えず、terminal 前 ACK をしない | 確認済み | 全シナリオの `cv.accepted_never_lost` / `cv.accepted_all_terminal`、2b〜2e の crash 時点の未 ACK、5a・5b、10 |
| 同期は開始状況に応じ OutcomeUnknown、非同期は policy どおり retry / DLQ | 確認済み | 1、3a、8a、8b（同期）。2a〜2e、3b、5、6、7、8c（非同期、今回の workload はすべて成功に収束。DLQ への収束は `scripts/queue/async-dispatch-e2e.sh` が既に確認） |
| 古い owner が新しい状態を上書きしない | 確認済み（2 gateway process・同じ `data_dir`） | 3a（同期 slot lease）、3b（非同期 claim） |
| UsageEvent 再送で二重計上しない | 確認済み | 7（ledger commit 後・cursor 前の crash と replay）、全シナリオの `cv.usage_counted_once` |
| 復旧後の孤児環境 / Secret / object を安全に回収 | 確認済み（process provider） / 確認済み（Firecracker、§8。回収した環境の host 原価は 2026-09-18 から計上、§8.1） | 10、1、6、全シナリオの `cv.no_orphan_processes` / `cv.no_open_environments` / `cv.no_secret_value_on_disk` |
| 制約・残課題の記録 | 記録済み | §6.2、§8、§9 |
| 故障マトリクス・機械可読結果・実行 profile・復旧時間の保存 | 保存済み | `docs/evidence/chaos-*/`（`results.jsonl`、`profile.json`、`summary.md`） |

## 8. Firecracker（KVM 最終検証 2026-09-17、一部）

`TSLS_PROVIDER=firecracker scripts/chaos/matrix.sh --only ...`（root、jailer・cgroup required、warm pool 有効、`CHAOS_TMP` は jailer の `chroot_base` と同じ file system）で、Firecracker で挙動か後始末の経路が違うシナリオと Firecracker 専用の guest OOM を Lima VM（nested virtualization、1 host）で 3 回実行した。証跡は `docs/evidence/kvm-final-chaos-20260917T152228Z/`（`summary.txt`、3 回分の `results.jsonl` と gateway log をすべて保存）。provider に触れない残りの 13 シナリオ（2a〜2e の failpoint、3b、4、5、6、7、9）は Firecracker では実行していない。

Firecracker での harness の違い（`scripts/chaos/lib.sh`、`scripts/kvm/provider-lib.sh`）: 環境の「終了した」判定は VMM / jailer の process に加えて jail（`/srv/jailer/firecracker/env-*`）・VMM cgroup（`/sys/fs/cgroup/tachyon/env_*`）・env dir が消えたこと、graceful stop の後に firecracker / jailer の process・cgroup・jail・`tsls*` tap・`table inet tachyon_egress` が 0、secret の走査は scratch dir（drive・console / fc log・snapshot dir を含む）と `/srv/jailer`、`examples/idempotent-async` は egress `restricted` で届く外部 store（`scripts/queue/effects-netns.sh`）に副作用を置く。

| シナリオ | Firecracker で違うところ | 結果（最終回 `run3-final/`） |
|---|---|---|
| 1 `sync_gateway_kill` | gateway の SIGKILL 後も VMM が残る | pass。VMM が生きていることを確認（`fault.vmm_survived_gateway_sigkill`）、再起動後の reclaim が 4 環境を terminate し VMM・jail・cgroup・env dir が消えた。dispatch 済みは `outcome_unknown`、未 dispatch は `failed`（`Host.Restarted`） |
| 10 `orphan_recovery_after_crash` | busy な microVM・未送信 outbox・scheduler lease・孤児 object を残した SIGKILL | pass（1 回目の初回試行だけ孤児 object が 0 件で前提不成立）。旧環境 4 → 0、outbox 5 → 0、lease 引き継ぎ、孤児 object 回収、secret 0 件（`/srv/jailer` を含む） |
| 3a `stale_owner_sync_lease` | B が A の VMM・jail・cgroup を終わらせる | 1・2 回目は回収・fencing の検査がすべて pass（B は lease + skew の後に回収、A の VMM を terminate、A の遅れた完了は拒否、A は fenced、B は 200）。**最終回は 2 試行とも失敗**: SIGSTOP した A が state.db の書込み lock を持ったまま止まり、B は A の SIGCONT までの 2 分間 heartbeat も回収もできず自分の lease を失った（503 `Host.AuthLeaseExpired`）。1 つの SQLite file を 2 process で共有する構成では、凍結した writer が他のすべての writer を止め、lease と fencing では防げない。どの transaction の途中だったかは特定していない（2026-09-18 に修正と再実行、§8.1: 止まった transaction を記録する guard、lock を持つ時間の短縮、止まっていなかった gateway だけが lease を取り戻す） |
| 8a / 8c worker 切断 | VMM process の SIGKILL（vsock も切れる） | pass。sync は `outcome_unknown`（client 502）、async は retry で成功・副作用 1 回、環境は host から消えた |
| 8b user process（→ guest OOM） | host から guest 内の process は kill できない。128 MiB の guest で 512 MiB を確保（`worker_user_process_oom_sync`、Firecracker 専用） | pass（1 回目は harness の不具合で中断、修正後の 2・3 回目は pass）。`Runtime.Crash`（signal 9）、guest console に OOM の行、VMM cgroup の memory.peak 129.7 MiB ≤ memory.max 192 MiB、同じ関数の次の invoke は 200 |
| 10 の Secret | drive・jail・snapshot dir | secret 値を含む file は全シナリオ・全回で 0 |
| 7 usage | cgroup の CPU usec / memory.peak を `provider_reported` で出す | 通常の停止（pool の回収・destroy-after-invoke・VMM kill・OOM）はすべて `provider_reported`。**残り**（2026-09-18 に修正、§8.1）: (a) reclaim が先に終わらせた fenced 環境を旧 owner が後で確定した場合と、graceful shutdown 中に起動途中で放棄された環境は `unknown`（cgroup が既に無い。値は作らない）、(b) reclaim / 起動時 reconcile が終わらせた環境には `EnvironmentStopped` 自体が出ない（最終回で sync_gateway_kill 7 環境中 4、orphan 14 中 6、stale owner 11 中 4）。その microVM の寿命全体の host 原価は計上されない。attempt 単位の `AttemptSettled` と「各 event 1 回」は全回で成立 |

### 8.1 修正後の再実行（2026-09-18、`fix/reclaim-metering-and-lock-stall`）

§8 が残した 2 つの不足（reclaim / 起動時 reconcile が終わらせた環境に `EnvironmentStopped` が出ない、`stale_owner_sync_lease` で凍結した gateway が `state.db` の書込み lock を持ち続けて相手も自分も lease を失う）を直し、同じ Lima VM で **Firecracker 3 回 × 4 シナリオ**（`stale_owner_sync_lease`・新しい `stale_owner_frozen_in_transaction`・`sync_gateway_kill`・`orphan_recovery_after_crash`）と、**process provider 3 回 × 7 シナリオ**（上の 4 つ + `baseline`・`db_locked_within_lease`・`db_locked_past_lease`）を実行した。**すべて初回試行で pass**。証跡: `docs/evidence/kvm-reclaim-lock-20260917T182451Z/`（`summary.txt`、3 回分）、`docs/evidence/chaos-reclaim-lock-20260917T182412Z/`（同）。

| 観点 | 2026-09-17（§8） | 2026-09-18 の再実行 |
|---|---|---|
| 回収・reconcile が終わらせた環境の `EnvironmentStopped` | 出ない（最終回で 7 中 4、14 中 6、11 中 4 が無し） | 新しい検査 `cv.every_terminal_environment_metered_once` が Firecracker 12・process 21 のシナリオ実行すべてで pass。terminal な環境 137（Firecracker）/ 462（process）のうち stop event の無いもの **0** |
| host 原価（VMM cgroup） | 通常の停止だけ `provider_reported`、reclaim 経路は計上なし、遅れた確定と起動途中の放棄は `unknown` | Firecracker の `EnvironmentStopped` 137 件中 134 件が `provider_reported`（cpu usec・memory.peak > 0）、3 件が `unknown`（誰も sample できなかった環境）。作った値は 0 件。reclaim が出した stop も cgroup の値を持つ（`stopped_by = reclaim`、`lifetime_source = ledger_reclaimer_clock`） |
| `stale_owner_sync_lease` | 最終回は 2 試行とも失敗（B が 2 分書けず自分の lease を失い、起きた A が B を回収） | Firecracker 3 回・process 3 回とも pass |
| 凍結した writer（新 `stale_owner_frozen_in_transaction`） | シナリオ無し（当たるかは運） | failpoint で A を `BEGIN IMMEDIATE` と `COMMIT` の間で必ず止める。止まった場所は 3 回とも `repository/sqlite/triggers.rs:522`（cron scheduler lease）で、保持時間は store の guard が `held_ms = 35402 / 35394 / 35417` と記録。SIGCONT 後: B は lease を取り戻して A を回収し fence されない、A は自分の stall を検知して fence・新規 503・誰も回収しない、fence された環境は terminate、invocation は 1 回だけ確定して以後変わらない |
| `db_locked_past_lease` | gateway が自分を fence し再起動が必要 | 再起動なしで復帰（`recovery.serves_without_restart`、process 3 回） |

残る限界（§6.2 と ADR-0003 に記録）: 書込み transaction の中で止まった process は、止まっている間 SQLite の書込み lock を手放さない。その間、他の gateway の 1 回の store 呼び出しは約 10 s で 503 になるが、store を複数回呼ぶ request 全体はそれより長くなりうる（再実行の観測: Firecracker では 3 回とも 20 s の client timeout、process では 5.3 s の 503 が 2 回・20 s timeout が 1 回）。

## 9. 対象外（この結果が何も言わないもの）

- 複数 host の HA、host の喪失、host 間の network partition、gateway と nats-server・object store が別 host にある構成。
- disk の破損（SQLite file・WAL・JetStream store の破損）、disk full、fsync の嘘、電源断（page cache の喪失）。SIGKILL は process の停止で、OS は書込みを失わない。
- 時刻の飛び、`max_clock_skew_ms` を超える時計のずれ（ADR-0003「残るもの」）。
- log sink（gateway の stdout / stderr）が詰まった場合。invocation log は `state.db` ではなく memory にあり、今回は試していない。
- nats-server の cluster・複製、JetStream の store 破損、認証情報の失効。
- usage journal / ledger の SQLite file 自体の lock・破損（§6.2）。
- TiDB など別の store adapter（PLT-4618 で進行中）。
- 本番・外部サービス（Linear の範囲外）。
- 負荷をかけた状態での故障。workload は 1 round あたり数件の小さなもの。
