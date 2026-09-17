# ADR-0018: invocation log は台帳と別の SQLite（`<data_dir>/logs/logs.db`）に、上限付き queue と writer thread で非同期に書き、保持期限と総量上限で消す。log store が使えなくても invocation は止めない（PLT-4628 follow-up）

## ステータス

Accepted（2026-09-17、PLT-4628 の follow-up）。ADR-0003 決定 5 の「log は memory のまま」を置き換える（ADR-0003 の末尾に addendum）。RFC（quantum-box/knowledge PR #284）§15 の「logs は保持期限を持つ」に対し、**保持期限と総量上限のある durable な log**までを入れる。OTel pipeline・検索・国内拠点の log 基盤は入れない（§非対象）。

実装:

- store: `crates/application/src/logs/store.rs`（`DurableLogStore`）、設定 `crates/application/src/logs/mod.rs`（`[logs]`）
- 差し替え点: `crates/application/src/app.rs`（台帳が durable のときだけ `Repositories.logs` を置き換える）、`LogRepository::query(tenant, invocation)`（`crates/application/src/repository/mod.rs`）
- `/readyz` の `logs`: `apps/gateway/src/handlers.rs::readyz`、終了時の flush: `apps/gateway/src/lib.rs::serve_with_console`
- metrics: `crates/application/src/metrics/{catalog.rs, render.rs}`（`tsls_logs_*`、`docs/metrics.md` §3.11）
- テスト: `crates/application/src/logs/tests.rs`、`crates/application/tests/pipeline.rs`（末尾の durable log 節）、`apps/gateway/tests/gateway_integration.rs::a_degraded_log_store_is_reported_without_failing_readiness`、`scripts/e2e/demo.sh`（restart step と `logs.db` の secret scan）

## コンテキスト

- PLT-4628 の log（`GET /v1/invocations/{id}/logs`、`tsls functions logs`、console の invocation 詳細）は `LogBuffer`（gateway の memory、invocation ごと 2000 行 / 1 MiB）にしか無く、**gateway の再起動で全部消えていた**。台帳（`state.db`）・usage journal・queue は durable になったのに、利用者が失敗の原因を調べる手段だけが再起動をまたげなかった（`docs/known-constraints-and-beta-gap.md` の「invocation log はメモリだけ」）。
- memory の上限は invocation 単位だけで、gateway 全体の上限も保持期限も無かった（プロセスが長生きすると増え続ける）。
- log は handler の出力そのもので量が多く、行ごとに `state.db` に書くと台帳の writer lock（`BEGIN IMMEDIATE`、PLT-4646 で上限付き待ちにした）を log が奪い合う。ADR-0003 が P1 の問題として挙げた「log 1 行ごとに台帳と同じ lock」を SQLite で再現することになる。
- log の本文は利用者のデータで、secret を含みうる（user code が自分で出力した場合）。host が secret 値を log に書かないことは既存の保証（T03）。

## 決定

1. **別 file にする。** `<data_dir>/logs/logs.db`（directory 0700、file 0600、`-wal` / `-shm` も SQLite が同じ mode）。`state.db` とは connection も lock も共有しない。schema version は `log_meta.schema_version`（1 から始まる独自の番号）で、binary より新しい file は開かず degraded にする。`state.db` の migration は無い。
2. **append は IO を待たない。** bridge session からの `LogRepository::append` は、行を 1 行の上限（`[limits] max_log_line_bytes`、既定 16 KiB。forwarder が char boundary で切り `truncated = true`、store も念のため同じ規則で切る）に収め、上限付きの memory queue（`[logs] queue_max_lines` 既定 20000 行 / `queue_max_bytes` 既定 16 MiB）に入れて返るだけ。queue の mutex は O(1) の push と writer の取り出しでしか持たない。
3. **1 本の writer thread が batch で commit する。** `flush_interval_ms`（既定 200 ms）ごと、または `flush_max_lines`（既定 1000 行）たまった時点で、queue の中身を 1 つの `BEGIN IMMEDIATE` トランザクションで書く。WAL、`synchronous = FULL`、`secure_delete = ON`、`auto_vacuum = INCREMENTAL`、busy timeout 1 s（log の遅れは writer だけが被る）。行は `seq INTEGER PRIMARY KEY AUTOINCREMENT` の順で返すので、flush をまたいでも append 順が保たれる。
   - **crash の窓**: commit 済みの batch は crash・電源断でも残り、file は壊れない（WAL + FULL）。失われうるのは queue に残っていた行（最長で直近 1 flush 間隔ぶん、または commit 途中の 1 batch）だけ。
   - **read-your-writes**: 読み取りは、その時点までに queue に入った行の commit を `read_flush_wait_ms`（既定 1 s）まで待ってから読む。log store が遅い・lock されているときは待ちを打ち切り、commit 済みの行だけを返す。
   - **終了**: gateway は graceful shutdown の最後に queue を commit してから writer を止める（`DurableLogStore::shutdown`）。最後の handle が drop されたときも同じ。
4. **上限は writer が、行と一緒に保存した数で判定する。** invocation ごとの行数 / bytes（`[limits] max_log_lines_per_invocation` 2000 / `max_log_bytes_per_invocation` 1 MiB）と attempt ごとの行数 / bytes（`[logs] max_lines_per_attempt` / `max_bytes_per_attempt`、既定は invocation の値と同じ）。数は `log_invocations` / `log_attempts` の行にあり、同じトランザクションで更新するので、**再起動をまたいでも上限は増えない**。超えた最初の 1 行の代わりに marker 行（stream `platform`、本文は `[tachyon] ` で始まる）を 1 度だけ書き、以降は数えるだけ。marker は上限に数えず、invocation あたり最大 16 行。上限に当たった invocation は `LogsResponse.dropped = true`。
5. **queue 満杯・log store 不可のときは捨てて数え、invocation は止めない。**
   - queue 満杯: append はその行を捨て `queue_full` で数え、invocation ごとに捨てた行数 / bytes を覚える（最大 4096 invocation。超えた分は数えるだけ）。次の flush で「`[tachyon] N log line(s) (B bytes) of this invocation were dropped: the log writer queue was full`」の marker を書く。
   - `logs.db` が lock されている・disk が満杯・開けない: その batch を捨てて `store_unavailable` で数え、同じく invocation ごとに覚えて、書けるようになった最初の flush で marker を書く。開けない file は 5 秒ごとに開き直す。connection の mutex は `sqlite_wait::lock_connection`（PLT-4646）で上限付きに待つ。
   - **readiness を落とさない**: `/readyz` は `logs`（`healthy`、`last_error`、queue・保存量・捨てた数）を出すが、`ready` の判定には入れない。log の欠落は invocation の結果（台帳）を変えず、利用者に `dropped` と marker で見えるので、log のために invocation を 503 にするより実行を続ける方を取る。監視は `tsls_logs_store_healthy` と `tsls_logs_lines_dropped_total` で行う。
   - 読み取りは、connection を得られない・file を開けないとき 503（`Host.StoreUnavailable`、`RepoError::Store`）。
6. **tenant を全行に持ち、全 query で絞る。** `log_lines` / `log_invocations` / `log_attempts` の主キー・index は `tenant_id` から始まり（`(tenant_id, invocation_id, attempt_id, seq)`、時刻範囲は `(ts_ns)`）、`query(tenant, invocation)` は `WHERE tenant_id = ? AND invocation_id = ?`。API は従来どおり `LogService` が台帳で invocation の tenant を確かめてから読み（他 tenant は 404）、その上で store 自体も tenant で絞る（二重）。memory buffer も同じ規則にした。
7. **保持期限と総量上限は writer が消す。** `retention_interval_seconds`（既定 60 s）ごとに:
   - **期限**: 最後の行が `retention_seconds`（既定 7 日、0 は期限なし）より古い invocation の log を、invocation 単位でまとめて消す。実行時間の上限（`[limits] max_execution_timeout_seconds`）は 7 日よりずっと短いので、実行中の invocation の log が期限で消えることはない。
   - **総量**: 保存した行の bytes（本文、marker 込み。SQLite の page と index は含まない）が `max_total_bytes`（既定 1 GiB、0 は上限なし）を超えている間、最初の行が古い invocation から順に消す。**台帳が terminal と答えた invocation だけ**を消し、実行中（台帳に非 terminal の行がある）・台帳を読めない invocation は飛ばす（`retention_skipped_non_terminal`）。したがって実行中の invocation の log だけで上限を超えている間は超えたままになる（例外として明記。実行中の log は invocation 上限で有界）。台帳に無い invocation の log は消してよい。
   - 消した後に `PRAGMA incremental_vacuum` で空き page を返す。
8. **`[store] backend = "memory"`（と `persist_state = false`）は memory buffer のまま。** テストと dev 用。`/readyz` は `logs.backend = "memory"`, `durable = false`。

## 選ばなかった案

- **`state.db` に表を足す**: 台帳の writer lock と busy timeout を log の書き込み量が奪い合い、PLT-4646 の上限付き待ちに log の遅れが乗る。backup / 保持期限の性質（台帳は保持期限なし、log は 7 日）も違う。
- **行ごとの同期書き込み**: bridge session の frame loop が disk を待ち、lock された file で invocation が止まる。1 行 1 fsync は log の多い handler で遅すぎる。
- **file（JSON lines）への追記と rotation**: tenant・invocation で引くための index、上限の数え直し、途中で切れた行の扱いを自前で持つことになる。SQLite の WAL で原子性と index を得る方が小さい。
- **log store 不可で readiness を落とす / invocation を拒否する**: usage journal（課金の根拠）と違い、log の欠落は結果を変えない。拒否すると log の障害が全 invoke の障害になる。

## 結果（consequences）

- 再起動後も `GET /v1/invocations/{id}/logs` と console が同じ行を返す（`invocation_logs_survive_an_application_restart`、E2E の「invocation logs survive a gateway restart」）。
- disk の使用量は `max_total_bytes` と SQLite の overhead（index で行 bytes の数十 % 程度、実測していない）で有界。ただし実行中の invocation の分は上の例外。
- 書き込みの遅れは queue で吸収され、遅すぎると行を捨てる（`tsls_logs_flush_lag_seconds`、`tsls_logs_queue_lines`、`tsls_logs_lines_dropped_total{reason="queue_full"}`）。
- `logs.db` は利用者の出力を平文で持つ。保存時暗号化は無く、権限（0600 / 0700）と保持期限だけで守る（`docs/threat-model.md` §14-20）。secure_delete で消した行は page から上書きされるが、checkpoint 前の WAL と file system の snapshot / backup には残る。
- 同じ `data_dir` を開く 2 つの gateway は同じ `logs.db` に書く（WAL、各自の writer thread）。上限の判定はトランザクション内の読み書きなので二重に数えない。

## 非対象

- 検索（本文の全文検索、時刻範囲・attempt での API 絞り込み。index はあるが API は invocation 単位のまま）、pagination、streaming（tail -f）。
- OTel / 国内 log pipeline への転送、gateway 自身の log（stdout）の保持、metadata 30 日などの RFC §15 の保持区分。
- 保存時暗号化、tenant ごとの保持期限・上限。
- chaos matrix（`scripts/chaos/scenarios.sh`）への「`logs.db` lock 中の invoke」シナリオ追加。同じ性質は unit / integration テスト（`a_locked_database_degrades_the_store_and_recovers`、`a_locked_log_store_never_fails_an_invocation`）で確かめ、chaos の `cv.no_secret_value_on_disk` は scratch directory 全体を読むので `logs.db` も対象に入っている。

## 受入の対応

| 要求 | 状態 | テスト |
|---|---|---|
| 再起動をまたいで残る | 実装済み | `logs::tests::logs_survive_a_restart_on_the_same_data_dir`、`tests/pipeline.rs::invocation_logs_survive_an_application_restart`、E2E restart step |
| flush をまたいだ順序・並行 | 実装済み | `logs::tests::{lines_keep_their_order_across_many_flushes, concurrent_appends_keep_every_line_in_order}` |
| 1 行上限・invocation / attempt 上限・marker | 実装済み | `logs::tests::{a_long_line_is_cut_at_the_line_limit_and_marked_truncated, the_invocation_cap_drops_counts_and_marks_once, the_attempt_cap_is_applied_per_attempt, caps_hold_across_a_restart, markers_per_invocation_are_bounded}` |
| 保持期限・総量上限（実行中は残す） | 実装済み | `logs::tests::{retention_by_age_removes_whole_invocations_past_the_horizon, the_size_cap_removes_the_oldest_terminal_invocations_and_keeps_running_ones}` |
| queue 満杯で invoke を待たせない | 実装済み | `logs::tests::a_full_queue_drops_and_counts_without_blocking_the_caller`（300 ms / batch の遅い writer） |
| lock・開けない file で degraded、invoke は成功 | 実装済み | `logs::tests::{a_locked_database_degrades_the_store_and_recovers, a_database_that_cannot_be_opened_drops_lines_and_never_fails_the_caller, a_newer_schema_is_refused_and_the_store_stays_degraded}`、`tests/pipeline.rs::a_locked_log_store_never_fails_an_invocation`、`gateway_integration.rs::a_degraded_log_store_is_reported_without_failing_readiness` |
| tenant 分離 | 実装済み | `logs::tests::another_tenant_never_reads_the_lines`、`repository::contract_tests::{memory,sqlite}::logs_are_bounded_per_invocation`（他 tenant の query） |
| 権限 0600 / 0700 | 実装済み | `logs::tests::the_directory_and_the_database_are_private` |
| host が secret 値を `logs.db` に書かない | 実装済み | `tests/pipeline.rs::secret_values_reach_the_log_store_only_when_user_code_prints_them`（user code が出力した場合だけ入る陽性対照つき）、E2E の secret scan（`logs/logs.db`、`-wal`） |
| metrics・`/readyz` | 実装済み | `metrics::tests`（`tsls_logs_*` の描画）、`gateway_integration.rs::{full_api_roundtrip, bootstrap_converges_on_a_state_file_left_behind_by_a_crash}`（`logs` block） |
