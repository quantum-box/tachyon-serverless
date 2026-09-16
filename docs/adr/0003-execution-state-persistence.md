# ADR-0003: 実行状態の永続化は control-plane と cell-local に分け、プロトタイプでは 1 ファイルの埋め込み SQLite に載せる（PLT-4618）

## ステータス

Proposed（2026-09-16）。P2（環境再利用・autoscaling・scale-to-zero）の前提として決める。本 ADR は文書だけで、Rust は 1 行も変えていない。実装と測定は P2 の担当 issue（PLT-4631 ほか）。

## コンテキスト

### P1 の形（コードを読んで数えた事実）

正本は `crates/application/src/repository.rs`（1183 行）。repository trait は 8 個、method は合計 34 個で、すべてを `InMemoryStore` 1 型が実装し、`Repositories::in_memory`（`crates/application/src/app.rs:131`）で束ねている。差し替え点は現状この 1 箇所だけである。

| trait | method 数 | method |
|---|---|---|
| `FunctionRepository` | 5 | `insert` / `get` / `find_by_name` / `list` / `update` |
| `RevisionRepository` | 5 | `allocate_number` / `insert` / `get` / `list_by_function` / `update` |
| `AliasRepository` | 3 | `get` / `list` / `modify`（closure を write lock 内で呼ぶ read-modify-write） |
| `InvocationRepository` | 8 | `insert` / `get` / `update` / `list_by_function` / `insert_attempt` / `get_attempt` / `update_attempt` / `attempts_of` |
| `EnvironmentRepository` | 7 | `insert` / `get` / `update` / `list_active` / `insert_lease` / `get_lease` / `update_lease` |
| `LogRepository` | 2 | `append` / `query` |
| `IdempotencyRepository` | 2 | `lookup` / `insert_bound` |
| `ArtifactOwnerRepository` | 2 | `claim` / `is_owned_by` |

保持と永続化の実際:

- 状態はすべて `RwLock<State>` 1 本の背後にある（`parking_lot`）。`State` は `durable: PersistedState` / `idempotency` / `leases` / `logs` の 4 つ。
- `PersistedState` は 9 個の map（`functions` / `revisions` / `revision_counters` / `aliases`（key は `"<function_id>/<alias>"` の文字列）/ `invocations` / `attempts` / `environments` / `idempotency`（pair の `Vec`）/ `artifact_owners`）。
- `mutate()`（呼び出し 14 箇所）は write lock 内で閉包を実行し、そのまま **`PersistedState` 全体**を `serde_json::to_vec_pretty` して `state.json.tmp` に書き、`rename` する。失敗しても `tracing::warn!` を出すだけで、呼び出し側は成功として扱う（best effort）。加えて毎回 `idempotency` の HashMap 全体を `Vec` に複製してから serialize する（`repository.rs:341-345`）。
- **Lease と log は永続化しない**。`insert_lease` / `update_lease` / `LogRepository::append` は同じ write lock を取るが、file は書かない。つまり lease はプロセスと同時に消え、log 行 1 行ごとに state.json の書き込みと同じ lock を奪い合う。
- `with_persistence` は起動時に `state.json` を読み、壊れていれば退避方法を示して起動を拒否し（`corrupt_state_file_is_refused_with_a_hint`。実際の事故は `docs/kvm.md` §7.1 の「ゼロ埋め」の行）、`reconcile_after_restart` で (a) ledger に無い invocation を指す idempotency key を捨て、(b) 非 terminal な invocation / attempt を `PlatformError` / `Host.Restarted` で失敗させ、(c) 非 terminal な environment を `Lost` にし、その結果を書き戻す。
- 終了時は `apps/gateway/src/lib.rs:158` の `persist_now()` でもう一度 flush する。
- secret 値は store に入らない（`docs/threat-model.md` §6-4、`docs/architecture.md` §5-2）。一方で invocation の inline `output` と input digest は入るため、file 権限は設定ファイルと同じ扱いにする（`docs/threat-model.md` §14-4）。

`crates/application/src/services/invoke.rs` から見た lease と epoch:

- Lease は Ready の後・handler 起動の前に 1 度だけ作る。`ExecutionLease::acquire(lease_id, env_id, attempt_id, tenant, env.epoch, execution_deadline_ts, now)`（`invoke.rs:1398-1407`）、`insert_lease`（1418）、結果確定後に `release` して `update_lease`（1632）、driver が panic したときも `cleanup_after_panic` で release する（798-801）。
- 結果の受理条件は `ExecutionLease::accepts(attempt_id, epoch)` だけ（`crates/domain/src/environment.rs:302`、`docs/threat-model.md` §6-3、T05）。
- **`epoch` は環境作成時に `1` で固定され、増やす経路がコードに無い**（`ExecutionEnvironment::request` の `epoch: 1`。`epoch` を進める method は domain に存在しない）。destroy-after-invoke で環境を再割り当てしないため、P1 ではこれで足りている。
- **`ExecutionLease::expires_at` / `is_active(now)` は domain の単体テスト以外から評価されていない**。timeout の実効は host の watchdog（`execution_deadline`）が担っており、lease の期限は「記録されているが使われていない」状態にある。
- `ReuseKey`（8 field。doc comment は RFC §5.3 を引く）は環境ごとに記録するだけで、**複合キーで引く経路がどの trait にも無い**。生成箇所は `invoke.rs:1052-1065` の 1 箇所で、`execution_role_version` / `configuration_version` / `network_policy_version` / `secret_binding_generation` は定数 1、`resource_profile_digest` は resources JSON の SHA-256、`runtime_profile` は `spec.runtime.protocol`。
- `EnvironmentRepository::list_active` を呼ぶのはテストだけである（`crates/application/tests/pipeline.rs:1302, 1564`）。production 経路は環境を列挙しない。

### なぜ P1 ではこれで足りたか

gateway は 1 プロセスで、容量は同一プロセス内の semaphore（`docs/architecture.md` §3-5）、環境は invoke ごとに作って壊す（`docs/threat-model.md` §5-6）。したがって「slot」は semaphore の permit と等しく、lease の所有者は常に同じプロセス内の driver task であり、fencing は `(attempt_id, epoch)` の比較だけで足りる。プロセスが落ちれば slot も lease も一緒に消えるので、再起動時に「全部失敗にする」`reconcile_after_restart` が整合する唯一の解になる。state.json は「再起動をまたいで **台帳**（誰が何を実行したか）を残す」ためだけに存在し、**調整（coordination）には一度も使われていない**。

### P2 が持ち込む要求

環境 pool・autoscaling・scale-to-zero では、環境と lease が 1 回の invoke より長生きし、PLT-4631 では複数の dispatcher が同じ slot を奪い合う。必要な原子性は次の 5 つで、いずれも「1 プロセス内の RwLock」では表現できない。

| # | 操作 | P1 の実装 | P2 が要求する原子性 | state.json で表現できるか |
|---|---|---|---|---|
| 1 | slot 取得 | プロセス内 semaphore + 毎回新規作成 | 「Idle かつ reuse key 一致の環境を **1 つだけ** Busy にし、epoch を 1 進める」= `(state, epoch)` の CAS | できない（2 プロセスが両方勝つ） |
| 2 | lease 更新と失効 | memory のみ。`expires_at` は評価されない | 期限切れの検出と回収が、別プロセスから **ちょうど 1 回**成立すること。renew は期限内のみ成功 | できない（lease がプロセス外から見えない） |
| 3 | pool membership | 環境は保存されるが列挙しない | min_ready / scale-to-zero の増減が再起動をまたいで一貫し、全 dispatcher に同じ集合が見えること | 保存はできるが、原子的な増減ができない |
| 4 | reuse key 検索 | 記録するだけ | 8 field の複合キー一致で候補を引く（index 付き） | できない（全件走査しか書けない） |
| 5 | Idempotency binding | `insert_bound` が 1 プロセス内で原子的 | 複数プロセスでも `(tenant_id, function_id, key)` が一意 | できない |

加えて **write amplification** が制約になる（後述の「write amplification」節）。pool と lease は invoke ごとどころか renew ごとに更新されるため、1 回の更新が台帳全体の再書き込みになる現在の形は P2 では成立しない。

## 選択肢

1. **`state.json` を維持する**（現状維持）。
2. **gateway プロセス内の埋め込み SQLite**（`<data_dir>/state.db`、WAL、書き込みは `BEGIN IMMEDIATE`）。
3. **TiDB**（RFC が前提にする control-plane store）。
4. **分割**: durable な control-plane 状態を DB に、短命な slot / lease 状態を cell 局所の store に置く（RFC §15 の形）。

## 比較

| 観点 | (1) state.json | (2) 埋め込み SQLite | (3) TiDB | (4) 分割（port の形） |
|---|---|---|---|---|
| 複数プロセスの原子性 | **無い**。各プロセスが全体を memory に持ち、全体を書き戻すので last-writer-wins で他方の台帳を黙って消す。file lock も無い | **単一 host なら有る**。同一 file を開いた複数プロセスが WAL + `BEGIN IMMEDIATE` で直列化。CAS は `UPDATE ... WHERE state = ? AND epoch = ?` の更新行数で判定。host をまたぐ調整とネットワーク FS は対象外 | **有る**（host もまたぐ）。トランザクションと一意制約 | store を決めない。**下に置く store が CAS を持たなければ満たさない**（cell-local を「プロセス内 map」にすると PLT-4631 は成立しない） |
| コード（本リポジトリ） | 0 | 8 trait の別実装 1 つ + slot/lease の新 port + schema と forward-only migration。`InMemoryStore` はテスト用の volatile 実装として残す | 上に加えて MySQL protocol の adapter。repository は現在 **同期** API で、invoke driver からも同期に呼ばれるため、async 化は `crates/application` 全体に波及する | port を割るだけなら小さい。実体は (1)〜(3) のどれかが要る |
| 運用 | 無し | 無し（file 1 つ。`tsls dev` と macOS でもそのまま動く）。依存 crate は増える（下記「結果」） | cluster（PD / TiKV / TiDB）または playground が要る。CI（`ubuntu-latest`、secret 無し）には service container、開発機には常駐 server が要る | 分割自体に運用コストは無い |
| repository trait への影響 | 無し | `modify`（closure を lock 内で呼ぶ形）は跨プロセスに写せないので、alias は `generation` の CAS（domain に既存）に置き換える。`load → 変更 → update` の 3 段（`invoke.rs` の `load_invocation` / `save_invocation`）は lost update になるため version 付き CAS にする。返り値の `Result<_, RepoError>` はそのまま使える | (2) と同じ変更に加えて全 method の async 化 | slot / lease / pool を repository から外して別 port にする。8 trait 側の signature は (2) と同じ変更で済む |
| 単一 host プロトタイプとの整合 | 整合するが P2 を実装できない | 整合する（ADR-0001 の「KVM のある Linux host 1 台で完結する」を壊さない） | 矛盾する（`docs/architecture.md` §1・§6、`docs/inventory-tachyon-apps.md` §3.3 の「既存 cluster に依存しない」） | 整合する |
| write amplification | 1 回の変更で台帳全体を再書き込み（後述） | 触れた行だけ。WAL の追記 + checkpoint | 触れた行だけ | store 依存 |
| 失敗時の挙動 | 書き込み失敗が warn だけで握り潰される。全書き換えのため、途中失敗で file を壊す窓が file size に比例する | 書き込み失敗がトランザクションの失敗として返る | 同左 | store 依存 |

## 決定

1. **状態を 2 つに割る（(4) の形を採用）。**
   - **control-plane（durable）**: Function / Revision / Alias / artifact ownership / Invocation ledger / Attempt / Idempotency binding。低頻度の書き込み、監査に使う、失ってはいけない。
   - **cell-local（実行時）**: 環境 pool の membership、slot の割り当てと epoch、Lease（`expires_at` と renew）。高頻度、cell（＝本プロトタイプでは host 1 台）の中でだけ意味を持つ。
   これを **port として分ける**。8 個の repository trait は control-plane 側に残し、slot / lease / pool は新しい port（例: `SlotStore`）として `crates/application` に定義する。invoke pipeline は port 越しにしか触らない。
2. **プロトタイプでは両方を 1 つの埋め込み SQLite file（`<data_dir>/state.db`、WAL）に載せる（(2)）。** 書き込みは `BEGIN IMMEDIATE`、slot / lease / alias / environment の更新はすべて CAS（`WHERE` に現在の `state` / `epoch` / `generation` を置き、更新行数 0 を「負け」として扱う）。同一 host の複数プロセスが同じ file を共有する前提で、ネットワーク FS 上の `data_dir` は対象外とする。
3. **TiDB（(3)）は将来の control-plane adapter に据え置く。** 同じ trait の別実装として後から追加できる形にすることが (1) の分割の目的であり、P2 で TiDB を導入しない。`docs/architecture.md` §6 の「TiDB 永続化は非対象」は変えない。
4. **`state.json` の write-through は廃止する（(1) は選ばない）。** 移行は下記の一度きりの import。
5. **Lease と pool は durable にする。** 現在 memory のみの lease を store に移し、`expires_at` を実際に評価する（取得・renew・失効の 3 経路）。log は memory のまま（`Limits` で上限があり、再起動で失われることは `docs/threat-model.md` §11 と API の `dropped` で既に観測可能）。UsageEvent も `InMemoryUsageSink` のまま（P1 非対象の範囲を広げない）。
6. **epoch を実際に進める。** 環境を再割り当てするたびに `epoch` を 1 進め、その増分を slot 取得と同じトランザクションで行う。`(attempt_id, epoch)` の fencing（`docs/threat-model.md` T05）は、再利用が入って初めて意味を持つ。

### この決定を選ぶ理由

- 5 つの要求（表の #1〜#5）を満たす最小の選択肢が (2) であり、(4) はその上で「どこまでを永続 DB に移すか」を後から変えられる唯一の形である。(1) は #1〜#5 のどれも満たさない。
- (3) は product としては正しいが、このリポジトリの前提（独立 public repo、cluster 非依存、KVM host 1 台、secret 無しの CI）と正面から衝突する。P2 の目的は「環境再利用と autoscaling が成立することを示す」ことであり、「TiDB 上で成立することを示す」ことではない。
- SQLite は単一 host での複数プロセス調整（PLT-4631 の要求）をちょうど満たし、それ以上のことは約束しない。約束しない範囲（複数 host）は「非対象」に明記できる。
- port を割っておけば、control-plane だけを TiDB に移す変更が invoke pipeline に波及しない。逆に割らないまま SQLite に移すと、後の TiDB 化で pipeline を再び触ることになる。

## `state.json` からの移行

1. SQLite 実装を既存 8 trait の裏に入れる。`InMemoryStore::new`（volatile）はテスト用に残し、`cargo test` が file を要求しないようにする。
2. 起動時、`<data_dir>/state.json` があり `state.db` に schema version 行が無ければ **一度だけ import する**: state.json を読む → 現在と同じ `reconcile_after_restart` を適用する → 1 トランザクションで全行を insert → schema version を記録 → `state.json` を `state.json.imported-<UTC>` に rename する（**削除しない**）。
3. `state.json` と、行のある `state.db` が同時に存在する場合は、両方の path を挙げて起動を拒否する（現在の「壊れた state.json を黙って捨てない」規則をそのまま延長する）。
4. 逆方向（`state.db` → `state.json`）は用意しない。旧版へ戻す手段は import 前の `state.json.imported-*` を戻すことだけであり、それを文書に書く。
5. schema は `schema_version` table と前進のみの migration（`NNN-*.sql`）で管理し、起動時に 1 トランザクションで適用する。
6. `docs/architecture.md` §4 の `data_dir` の説明（`artifacts` と `state.json`）は、実装した issue が `state.db` に更新する。本 ADR では変えない。

## 受入条件（この決定が「効いた」と言える条件）

実装 issue は次を自動テストで示す。A1・A2・A6 は **スレッドではなく OS プロセスを複数**起動して確かめる（現在の形で失敗し、決定後の形で通ることが要件）。

| # | 条件 |
|---|---|
| A1 | 同じ 1 slot を N プロセスが同時に取りに行くと、成功はちょうど 1 つ。敗者は「取得失敗」を受け取り、勝者の `epoch` は取得前の値 + 1 になる |
| A2 | lease を持ったプロセスが release せずに終了した場合、他プロセスは `expires_at` **より前には**回収できず、以後ちょうど 1 回だけ回収できる。回収後に旧 `(attempt_id, epoch)` を持つ結果が届いても `ExecutionLease::accepts` が false を返す |
| A3 | 期限切れ後の renew は失敗し、lease を復活させない |
| A4 | gateway 再起動後、Ready / Idle の環境が reuse key ごと pool に残る。live な lease を持たない Busy 環境は現在と同じく `Lost` + `Host.Restarted` に落ちる |
| A5 | reuse key の検索が 8 field の完全一致でだけ候補を返す（1 field 違えば候補 0）。10k 行で index が使われる（`EXPLAIN QUERY PLAN` が全走査でないこと） |
| A6 | 同じ Idempotency-Key を 2 プロセスが同時に投入すると、1 つだけが Inserted、他方は同じ invocation を指す Existing を得る（`docs/threat-model.md` §10 の契約を跨プロセスで維持） |
| A7 | 1 invocation あたりの書き込み量が台帳サイズに比例しない: M 件を記録した後の 1 件の書き込み量が、1 件目の書き込み量の定数倍に収まる（現在は比例して増える。下表） |
| A8 | P1 の gateway が書いた `state.json` を空の `state.db` に import でき、ledger が一致し、file が rename され、2 度目の起動で再 import しない。壊れた `state.json` は今までどおり起動を拒否する |
| A9 | 既存の repository テスト 8 本（`function_name_unique_per_tenant`、`idempotency_key_is_bound_with_its_invocation`、`dangling_idempotency_entries_are_dropped_on_restart`、`artifact_ownership_is_per_tenant_and_persisted`、`state_without_artifact_owners_still_loads`、`logs_are_bounded_per_invocation`、`persistence_roundtrip_and_restart_reconcile`、`corrupt_state_file_is_refused_with_a_hint`）が、volatile 実装と SQLite 実装の両方に対して通る共通 suite になる |

## write amplification（何を計算し、どう計算したか）

**測定していないこと**: gateway を動かしていないので、時間（ms）も IOPS も測っていない。以下はサイズの算術であり、benchmark ではない。

**数え方**: 1 回の成功した invoke が起こす write-through は、`crates/application/src/services/invoke.rs` の次の **12 箇所**である（行番号は本 commit 時点）。326 `insert_bound` / 1074 environment `insert` / 1085 `save_env`(provisioning) / 1227 `save_env`(initializing) / 1291 `save_env`(hello の evidence) / 1353 `save_env`(ready) / 1417 `insert_attempt` / 1421 `save_invocation`(running) / 1422 `save_env`(busy) / 1653 `update_attempt` / 1674 `save_invocation`(terminal) / 1724 `save_env`(stopped)。失敗経路はこれより少ないか同程度。lease の `insert_lease` / `update_lease` と log の各行は file を書かないが、同じ write lock を取る。

**サイズの出どころ**: `docs/evidence/<run>/invocations.json`（Invocation と Attempt の API 表現。inline `output` を含む）と同 run の `revisions-*.json` / `aliases-*.json` を `json.dumps(indent=2)` で測り直した（`write_state` が `to_vec_pretty` を使うため）。環境 record だけは evidence に無いので、`crates/domain/src/environment.rs` の `ExecutionEnvironment` の field を、同 run の実際の `boot_evidence` を使って組み立てて測った（**この 1 項目だけ推定値**）。`functions` の行は evidence に無いので base は下限である。

| 値 | `20260915T171631Z-firecracker` | `20260915T171415Z-process` |
|---|---|---|
| invocation 件数 / attempt 件数 | 39 / 33 | 51 / 33 |
| invocation 1 件（attempt 込み、pretty） | 中央値 2,686 B（最小 628 / 最大 2,788） | 中央値 1,953 B（最小 616 / 最大 2,151） |
| environment 1 件（推定、pretty） | 1,872 B | 1,304 B |
| revision + alias の合計（run 終了時） | 19,602 B（revision 18 / alias 3） | 25,737 B（revision 24 / alias 3） |
| run 終了時の台帳（推定 state.json サイズ） | 約 180 KiB | 約 169 KiB |
| 1 invocation 目の 12 回で書く量 | 282 KiB | 340 KiB |
| 最後の invocation の 12 回で書く量 | 2.11 MiB | 1.98 MiB |
| run 全体で書いた量（合計） | 約 49 MB | 約 63 MB |

同じ中央値の record が 1000 件積まれた場合（`state.json` に retention が無いので消えない）: 台帳 4.37 MiB、1000 件目の 1 invocation だけで 52 MiB を書き、合計で約 27.6 GB になる。台帳サイズに比例して 1 件あたりのコストが増えるため、書き込み総量は件数の 2 乗で伸びる。

副作用として次が付く。

- 書き込みはすべて `RwLock` の **write lock 内**で行われる。log 1 行の append も同じ lock を取るため、台帳が大きくなるほど log 出力の多い handler が pipeline 全体を待たせる。
- 毎回 idempotency map 全体を `Vec` に複製してから serialize する（`repository.rs:341-345`）ので、key 数にも比例した複製が乗る。
- 全体を書いて `rename` するため、書き込み途中で disk が尽きると file 全体が壊れる。実際に起きた記録が `docs/kvm.md` §7.1（ゼロ埋めされた `state.json` で起動を拒否）にある。
- 書き込み失敗は warn だけで、呼び出し側は成功として続行する。すなわち P1 の台帳の耐久性は「best effort」である。

P2 では pool と lease の更新（renew は invoke より高頻度になりうる）が同じ経路に乗るため、この形は維持できない。受入条件 A7 はこの点を検査する。

## 結果（consequences）

- `crates/application` に SQLite adapter（依存: `rusqlite` を root `Cargo.toml` の `[workspace.dependencies]` に追加。bundled feature は C の SQLite を build する）。`Cargo.lock` には既に `cc` と `ring` が入っている（rustls 経由）ため、host build に C compiler が要るのは現状と変わらない。guest 側（`crates/runtime-bridge`、`crates/sdk`、`examples/*`、各 provider）は `crates/application` に依存しないので、CI の musl guest build（`.github/workflows/ci.yml` の `guest-musl-build`）には入らない。
- `Repositories::in_memory` の 1 箇所が差し替え点なので、application の service 層は変更なしで新実装に載せ替えられる。ただし `AliasRepository::modify`（closure を lock 内で呼ぶ）と、`load → 変更 → update` の 3 段（`invoke.rs` の `load_invocation` / `save_invocation`）は CAS へ書き換える必要がある。ここが今回の決定で唯一 pipeline に波及する部分である。
- ID の prefix はどれも変えない。`env_` が tachyon-apps の `EnvironmentVariableId` と衝突する件（`docs/inventory-tachyon-apps.md` §3.1, §7-1）は、**本リポジトリが自分の store（schema）を所有し、tachyon-apps と同じ table 空間を共有しない**ことで解消とする。将来どうしても同じ store に流す場合にだけ、出所で修飾する（rename しない）。
- `docs/threat-model.md` §14-4（state file の権限）は `state.db` にそのまま引き継ぐ。secret を書かない規則（§6-4）も同じ。
- `reconcile_after_restart` の意味が変わる。lease が durable になると「in-flight だったものを全部失敗にする」必要はなくなり、「live な lease を持たない Busy 環境だけ `Lost` にする」に狭められる。受入条件 A4 はこの差分を固定するためにある。
- `docs/acceptance.md` の PLT-4618 の行（#5 `state.json` 永続化、#6 TiDB migration、#7 `env_` prefix）は、実装が入った後に integrator が更新する。本 ADR では触らない。

## 非対象（プロトタイプ）

- 複数 host / 複数 cell の調整、cell 間の移送、host をまたぐ pool。SQLite は同一 host の同一 file 上でしか調整しない。
- TiDB の導入・deployment・migration の実運用（adapter を後から足せる形にするところまで）。
- control-plane の HA、backup / restore、PITR、retention（`state.json` に retention が無いのと同じく、当面は増える一方であることを明示する）。
- log と UsageEvent の永続化。log は `Limits` で上限付きの memory のまま、UsageEvent は `InMemoryUsageSink` のまま。
- 保存時暗号化。`docs/threat-model.md` §14-4 の「file 権限を設定ファイルと同じ扱いにする」前提を変えない。
- ネットワーク FS 上の `data_dir`（SQLite の lock が成立しない構成）。preflight で検出はしない。対象外とだけ宣言する。

## 参照

- 基準 RFC: quantum-box/knowledge PR #284「Tachyon Serverless 全体設計 RFC v0.1」（`docs/architecture.md` の先頭、`README.md` の「設計文書」）。**本リポジトリが節番号まで引用しているのは `crates/domain/src/environment.rs` の `ReuseKey`（RFC §5.3）だけ**である。本 ADR の「durable な control-plane と cell 局所 store の分割」は RFC §15 の形として PLT-4618 の指示で与えられたものを指しており、RFC 本文はこのリポジトリからは参照できない。RFC 本文と食い違う場合は RFC を正とし、本 ADR を改版する。
- コード: `crates/application/src/repository.rs`（8 trait / 34 method、`InMemoryStore`、`mutate`、`write_state`、`reconcile_after_restart`）、`crates/application/src/app.rs:120-131`（`with_persistence` の分岐と `Repositories::in_memory`）、`crates/application/src/services/invoke.rs`（write-through 12 箇所、lease と epoch）、`crates/domain/src/environment.rs`（`ExecutionEnvironment` / `ReuseKey` / `ExecutionLease`）、`apps/gateway/src/lib.rs:158`（終了時 flush）。
- 文書: `docs/architecture.md` §3・§5・§6、`docs/threat-model.md` §5・§6・§10・§14、`docs/inventory-tachyon-apps.md` §2・§3.1・§7、`docs/adr/0001-execution-provider-firecracker-first.md`（単一 host で完結する前提）、`docs/adr/0002-process-provider-dev-only.md`。
