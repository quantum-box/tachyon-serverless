# DB index と expand / contract のレビュー（PLT-4618）

- 対象: `crates/application/src/repository/sqlite/migrations/001〜008`（製品の store）と、その TiDB 版 `crates/application/src/repository/tidb/migrations/001〜008`（試験専用 adapter、ADR-0003「TiDB 検証（2026-09-17）」）
- 計測: `scripts/db/tidb-verify.sh` の `repository::tidb::tests::{index_review_sqlite, index_review_tidb}`。同じ seed data を両方に入れ、SQLite は `ANALYZE` 後の `EXPLAIN QUERY PLAN`、TiDB は `ANALYZE TABLE` 後の `EXPLAIN ANALYZE`（削除系は実行しないよう `EXPLAIN`）
- 生の plan: `docs/evidence/tidb-20260917T103330Z/explain-sqlite.md`、`explain-tidb.md`
- 環境: TiDB v8.5.8（PD + TiKV + TiDB 各 1、macOS arm64、127.0.0.1）、SQLite 3.53.2（rusqlite bundled）

## 1. seed data

| 表 | 行数 | 分布（hot query の選択性を決めるもの） |
|---|---:|---|
| `invocations` | 10,000 | function 100 個に均等、98% terminal、owner 20 個、1/3 の terminal に `output_expires_at` |
| `environments` | 2,000 | state 5 種に均等（idle 400）、owner 20 個、reuse key は revision 100 × configuration_version 3 |
| `leases` | 10,000 | 1% 未 release |
| `idempotency` | 10,000 | 98% に `expires_at` |
| `outbox` | 10,000 | 5% 未送信 |
| `triggers` / `trigger_fires` | 200 / 10,000 | cron と webhook が半々、webhook の fire に signature |

`body` / `payload` は約 1 KiB の JSON。2 byte の placeholder では TiDB の cost model が全件走査を過小評価するため、実際の行の大きさに寄せた。

## 2. hot query ごとの access path

「要求経路」は API / invoke の 1 リクエストごとに走る query、「周期処理」は sweep・retention・poller。要求経路で index を使わない plan はテストが失敗にする。周期処理は plan を記録し、下の判断を書く（`HotQuery::sweep`）。

| query（コード） | 種別 | SQLite | TiDB v8.5.8 | 判断 |
|---|---|---|---|---|
| reuse key で pool から取る（`SlotStore::claim_for_reuse`） | 要求経路 | `SEARCH environments USING INDEX environments_reuse_key`（8 列の等値） | `IndexRangeScan environments_reuse_key` → `TableRowIDScan`、act 7 行 → `SelectLock`（`FOR UPDATE`） | 良い。`ORDER BY id LIMIT 1` は index 末尾の `id` で満たせるが TiDB は 7 行を読んで並べる。1 key あたりの idle は `max_idle_per_key` 以下なので問題にならない |
| function の invocation 一覧（`InvocationRepository::list_by_function`） | 要求経路 | `SEARCH invocations USING INDEX invocations_function_accepted (function_id=?)` | `IndexRangeScan invocations_function_accepted`、`limit embedded`、act 50 | 良い。`ORDER BY accepted_at DESC, id DESC` を index 順で満たす |
| idempotency の照合（`IdempotencyRepository::{lookup, insert_bound}`） | 要求経路 | 主キー autoindex + `invocations` の主キー | `Point_Get PRIMARY(tenant_id, function_id, idem_key)` → `IndexJoin` で `invocations.PRIMARY` | 良い |
| 未 release の lease（`acquire` / `release_to_pool`） | 要求経路 | `COVERING INDEX leases_environment_released` | `IndexRangeScan leases_environment_released`（index だけで完結） | 良い |
| dispatcher の lease（`SlotStore::heartbeat`） | 周期（数秒ごと、dispatcher 数に比例） | `leases_owner_released` + temp B-tree で `ORDER BY id` | `IndexRangeScan leases_owner_released`、act 100 | 良い。並べ替えは保持 lease 数だけ |
| trigger fire の重複排除（キー） | 要求経路 | 主キー autoindex | `Point_Get PRIMARY(trigger_id, fire_key)` | 良い |
| webhook の再送検出（signature） | 要求経路 | `trigger_fires_signature` | `Point_Get trigger_fires_signature` | 良い。SQLite の部分 unique index は TiDB では通常の unique key（NULL は重複扱いされない）で同じ保証 |
| 回収: 期限切れ lease（`reclaim_expired` 2） | 周期 | `leases_released_expires` | `IndexRangeScan leases_released`（act 100） | 良い。TiDB は `released` だけの index を選んだ。未 release が 1% なので妥当 |
| 回収: 死んだ dispatcher の invocation（`reclaim_expired` 3） | 周期 | `invocations_owner_terminal` | `IndexRangeScan invocations_owner_terminal` | 良い |
| idle 一覧（`SlotStore::list_idle`） | 周期（pool sweep） | `environments_state_idle_since` | **`TableFullScan environments`**（2,000 行、act 100） | 2,000 行では TiDB が全件走査を選ぶ。cell 1 つの環境数は host の容量で頭打ち（数百〜数千）なので許容。環境が増える構成では `(state, owner_id, idle_since, id)` の index を追加して再計測する |
| outbox の claim（`sqlite/outbox.rs`） | 周期（publisher の poll） | `outbox_sent_next` + temp B-tree | **`TableFullScan outbox`**（10,000 行、`TopN`） | 要対応（TiDB に載せるとき）。`next_attempt_at <= ?` の範囲と `ORDER BY created_at` が別の index にあり、未送信 5% でも TiDB は走査を選んだ。送信済み行は retention で消えるので表は小さく保たれる前提だが、TiDB adapter で outbox を実装するときは `(sent, created_at, event_id)` で順序を満たし `next_attempt_at` を filter にする形と、`sent_at` を先頭にした retention 用 index を合わせて再計測する |
| 期限の来た cron（`sqlite/triggers.rs`） | 周期 | `triggers_due` | **`TableFullScan triggers`**（200 行） | 200 行では妥当。trigger は function あたり上限付き |
| retention: inline 出力（`purge_expired_outputs`） | 周期 | `invocations_output_expires` | **`TableFullScan invocations`**（10,000 行、該当 457） | 要対応（大きな台帳で）。`output_expires_at` の範囲 `<= now` は「過去全部」なので、期限切れが溜まるほど走査と差がなくなる。TiDB では TTL table（`TTL = ... TTL_ENABLE`）か、バッチ（`LIMIT` 付き）にして index range を強制する |
| retention: idempotency（`purge_expired_idempotency`） | 周期 | `idempotency_expires` | `IndexRangeScan idempotency_expires` | 良い |
| retention: 送信済み outbox | 周期 | **`SCAN outbox`** | **`TableFullScan outbox`** | 両方とも走査。`sent_at` を先頭にした index が無い。95% が送信済みなので現行 plan は最適に近い。上の outbox の項と合わせて扱う |
| retention: trigger fire | 周期 | `trigger_fires_created` | **`TableFullScan trigger_fires`**（該当 2,520 / 10,000） | 該当率 25% では走査が妥当。retention が回っていれば該当は少数になる |

まとめ: 要求経路の 6 query と heartbeat・回収の 3 query は SQLite・TiDB とも index（または point get）を使う。TiDB が全件走査を選んだのは周期処理の 6 query（送信済み outbox の retention は SQLite も走査）で、いずれも 10,000 行以下の表では妥当な cost 判断だが、台帳が大きくなる構成では outbox の claim と inline 出力の retention が最初に効く。

## 3. TiDB での hotspot と主キーの選択

- **ID が時刻順（ULID 小文字、`<prefix>_<26 文字>`）**: 連番と同じく、主キー順に書き込みが末尾の region へ集中する。TiDB 版 migration は 1 invocation ごとに行が増える表（`invocations`、`attempts`、`leases`、`environments`、`outbox`、`invocation_inputs`、`object_refs`、`trigger_fires`、`async_dispatch`、`dead_letters`、`redrives`、`functions`、`revisions`）を `PRIMARY KEY (...) NONCLUSTERED` + `SHARD_ROW_ID_BITS = 4`（`invocations`、`attempts`、`leases`、`outbox`、`invocation_inputs`、`async_dispatch` は `PRE_SPLIT_REGIONS = 2` も）にした。行データは `_tidb_rowid` の上位 bit で 16 shard に散る。
- **それでも残るもの**: NONCLUSTERED の主キー index 自体（`id` の unique index）は ULID 順のままなので、index の書き込みは末尾 region に集まる。`invocations_function_accepted` のように先頭が function id の index は function ごとに分散するが、`outbox_sent_next`・`invocations_terminal` のように低 cardinality の列 + 時刻の index は同じ区間に集中する。1 host の cell（本 prototype）では問題にならない書き込み量だが、control plane を TiDB に載せる時点で次のどれかを選ぶ:
  1. ID を shard 付きにする（例: `inv_` の後ろに ULID の random 部の先頭 1 byte を前置した列を clustered key にする。外に出す ID は変えない）。
  2. `AUTO_RANDOM` の BIGINT を clustered 主キーにし、ULID は unique key にする（ULID の unique index の hotspot は残る）。
  3. 低 cardinality 先頭の index（`*_terminal`、`outbox_sent_*`、`leases_released`）を、tenant や owner を先頭に置いた形へ置き換える。
- **clustered を残した表**: `store_meta`、`schema_version`、`revision_counters`、`aliases`、`idempotency`、`artifact_owners`、`config_publication`、`object_tombstones`、`dispatchers`、`triggers`、`trigger_scheduler`。書き込みが少ないか、先頭が tenant / function で分散する表。
- 検証した設定の既定値（`versions.txt`）: `tidb_enable_clustered_index = ON`、`tidb_txn_mode = pessimistic`、`transaction_isolation = REPEATABLE-READ`（adapter は接続ごとに READ-COMMITTED にする。理由は ADR-0003「TiDB 検証」）。

## 4. TTL / retention

| データ | 現在の仕組み | TiDB での扱い |
|---|---|---|
| inline 出力 | `[store] output_retention_seconds` 後に digest へ置換（`purge_expired_outputs`） | 更新であって削除ではないので TTL table は使えない。`LIMIT` 付きのバッチ更新にし、上の走査を避ける |
| idempotency binding | `expires_at` 経過で削除（`purge_expired_idempotency`） | TTL table にするには `TTL` 句が時刻型の列を要するので、`expires_at` が文字列のままでは使えない。`DATETIME(6)` 列の追加（expand + backfill）が要る。それまでは現行の purge（`idempotency_expires` を使う）で足りる |
| 送信済み outbox、trigger fire、collection tombstone、dead letter | 各 repository の purge | 同上。TTL table を使うなら時刻列を `DATETIME(6)` にする migration を expand（新列 + backfill）→ 読み書き切り替え → 旧列 drop の 3 段で行う |
| `dispatchers` | retention なし（ADR-0003 PLT-4631「残るもの」） | 変わらず未着手 |

timestamp を固定幅 RFC 3339 文字列にしている（SQLite と共通の encoding、文字列順 = 時刻順）ことが TTL table を使えない理由である。TiDB 版でも文字列のまま揃えたのは、adapter 間で行の意味を変えないため。

## 5. expand / contract の規則（両方の store）

1. **expand**: 新しい表、nullable または定数 default の列、index の追加だけ。旧 binary は拡張済みの schema で動く（SQLite: 列を名前で指定して読み書きしている。TiDB: 同じ）。
2. **backfill**: 値を計算で埋める必要があるもの（`output_expires_at`、`idempotency.expires_at`）は SQL ではなく store を開くときのコードで行う（保持期間は設定であって schema ではないため）。
3. **切り替え**: 読み書きを新しい列に切り替えるのは expand と同じ release。
4. **contract**: 列の削除・型変更・名前変更は、それを読む binary がサポート外になった後の別 release の別 migration。現在の 001〜008 に contract は無い（テスト `every_statement_is_idempotent_and_additive` が TiDB 版に `DROP` / `MODIFY` / `CHANGE` / `RENAME` / `TRUNCATE` が無いことを検査する）。
5. **失敗時**:
   - SQLite: DDL が transactional なので、未適用の migration は 1 transaction で適用され、失敗すれば前の schema に戻る（`sqlite/tests.rs::a_failing_migration_leaves_the_previous_schema_intact`）。
   - TiDB: DDL は statement ごとに commit され巻き戻らない。代わりに (a) すべての statement を `IF NOT EXISTS` で冪等にし、(b) 1 つの `ALTER TABLE` の複数 clause は TiDB の multi-schema change として原子的に適用され、(c) `schema_version` は全 statement の成功後にだけ書く。失敗した migration の途中までの結果は expand だけなので旧 binary はそのまま動き、再実行で残りが適用される（`tidb/tests.rs::a_failing_tidb_migration_keeps_the_previous_schema_usable_and_can_be_rerun`: 2 つ目の statement で失敗 → version は据え置き、1 つ目の表は残る、失敗した `ALTER` の列は 1 つも入らない、旧 version で読み書きできる、直した migration で完了）。
   - TiDB v8.5.8 は同じ `ALTER TABLE` で追加した列に index を張ると ERROR 1072 を返す（実測）。列と index は別の statement にした（002、003）。
6. **同時実行**: TiDB では `GET_LOCK('tsls_schema_migration')` で migrator を 1 つにする（`concurrent_tidb_migrators_apply_each_migration_once`）。SQLite は `BEGIN IMMEDIATE`。
