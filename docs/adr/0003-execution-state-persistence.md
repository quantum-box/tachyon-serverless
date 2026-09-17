# ADR-0003: 実行状態の永続化は control-plane と cell-local に分け、プロトタイプでは 1 ファイルの埋め込み SQLite に載せる（PLT-4618）

## ステータス

Accepted（2026-09-17、PLT-4618 で決定 2〜4 と移行を実装、PLT-4631 で決定 1・5 と受入条件 A1〜A3・A6 を実装、PLT-4618 の TiDB 検証を 2026-09-17 に追加。製品の store は SQLite のまま）。提案は 2026-09-16。実装の内容と、決定・受入条件のうちまだ入っていないものは「実装メモ（PLT-4618、2026-09-17）」と「実装メモ（PLT-4631、2026-09-17）」にある。後者が前者の表を上書きする。

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

## 実装メモ（PLT-4618、2026-09-17）

「決定」2〜4 と「`state.json` からの移行」を実装した。決定 1・5・6 と受入条件のうち、この issue で入れていないものは下の表に理由とともに残す。以下の「コンテキスト」節は決定時点（P1、`repository.rs` 1 ファイル）の記述で、書き換えていない。

### 入ったもの

| 項目 | 実装 |
|---|---|
| store | `crates/application/src/repository/sqlite/`（`SqliteStore`）。依存は `rusqlite`（`bundled`）。repository API は同期のまま。1 store = 1 connection（mutex）で、書き込みは 1 操作 1 トランザクション（`BEGIN IMMEDIATE`）、`journal_mode = WAL`、`synchronous = FULL`、`busy_timeout = 5s` |
| 設定 | `[store] backend = "sqlite" \| "memory"`（既定 sqlite）、`[store] output_retention_seconds`（既定 7 日、0 は無期限）。`BootstrapOptions::persist_state = false` は設定に関係なく volatile |
| volatile 実装 | `InMemoryStore` は file を一切書かない。`state.json` の write-through と `persist_now` は削除した |
| schema | `repository/sqlite/migrations/001_initial.sql`（全表と index）、`002_output_retention.sql`（expand のみ: 列と index の追加）。`schema_version` に記録し、未適用分を起動時に 1 トランザクションで適用する。binary より新しい schema は起動を拒否する |
| 行の形 | 各行は domain object の JSON を `body` に持ち、列は検索・一意性・CAS・保持期限に使うものだけ。timestamp は固定幅の RFC 3339 UTC 文字列（文字列順 = 時刻順）。reuse key の版数は 64 bit をそのまま signed 列に入れる（一部が digest の切り詰めで `i64` を超えるため。等値比較しか使わない） |
| CAS | alias は `AliasRepository::{insert, compare_and_set}` に置き換えた（closure を lock 内で呼ぶ `modify` は削除）。`AliasService::apply` は「読む → domain で更新 → 読んだ generation で CAS」で、期待 generation 付きの更新は負けたら `Conflict`、無条件の publish は読み直して再試行する。environment は `epoch` と terminal flag（claim / release / sweep は `state` も）、invocation / attempt は terminal flag、lease は released flag を `UPDATE ... WHERE` に置く |
| 行の不変条件 | `repository/guard.rs` を両 store が使う: 親（function / revision / invocation / environment）が存在すれば tenant が一致すること、alias は同じ function の存在する revision だけを指すこと、identity（id・tenant・親 id・number・spec・digest・reuse key・作成時刻など）を変えないこと、terminal 行・Ready/Failed revision・release 済み lease・削除済み function を書き換えないこと（同一内容の再書き込みは no-op）、別 epoch の environment コピーを書かないこと、インライン出力が `limits.max_response_bytes` を超えないこと。違反は `RepoError::Refused`（API では 409）、id 重複は `RepoError::Conflict` |
| 本文と保持期限 | 入力は digest とサイズだけ（従来どおり）。出力本文は `[invoke] inline_output_max_bytes` 以下のときだけ行に入り、terminal になった時点から `output_retention_seconds` 後に `PayloadRef::Digest`（sha256 とサイズ）へ置き換える（起動時と gateway の 10 分ごとの timer。`SqliteStore::purge_expired_outputs`）。置き換え後は idempotent replay と `GET /v1/invocations/{id}` が出力本文を返さない。secret 値はどの行にも無い |
| 再起動 | P1 の規則をそのまま SQL に移した（`repository/restart.rs`）: `Running` → `OutcomeUnknown{Host.Restarted}`、`Accepted`/`Queued` → `Failed{PlatformError}`、attempt は invocation に従う、非 terminal の environment は `Lost`、Invocation の無い idempotency key は削除。追加で、未 release の lease を release する（P1 では lease がプロセスと一緒に消えていた） |
| 移行 | `state.json` があり、DB が空なら 1 トランザクションで取り込み、`state.json.imported-<UTC>` に rename する。取り込んだ file の sha256 を `store_meta` に記録し、rename だけが失われた場合（同じ bytes の `state.json` が戻っている）は rename だけやり直す。行のある DB と別の `state.json` が並んでいれば両方の path を挙げて拒否、壊れた `state.json` は従来の hint 付きで拒否する。取り込み後の reconcile は上の「再起動」と同じ 1 回の処理で行う（「state.json を読む → reconcile → insert」の順ではなく「insert → reconcile」だが、結果の行は同じ） |
| 権限 | `state.db` を新規作成するときは mode `0600`。`-wal` / `-shm` は SQLite が DB file と同じ mode で作る。既存 file の mode は変えない |

### 決定・受入条件との差分

| 項目 | 状態 | 内容 |
|---|---|---|
| 決定 1（slot / lease / pool を別 port `SlotStore` に分ける） | 未着手 → **PLT-4631 で実装**（下記） | PLT-4632 が pool を `EnvironmentRepository::{claim_for_reuse, release_to_pool, take_idle_for_termination}` として先に入れており、この issue ではその signature を変えていない。両方とも同じ SQLite file に載るので、分割は TiDB adapter を作るときの作業として残る |
| 決定 5（lease の `expires_at` を取得・renew・失効で評価する） | 一部 → **PLT-4631 で実装**（下記） | lease は `leases` 表に永続化される。期限の評価・renew・他プロセスからの回収は未実装（PLT-4631） |
| 決定 6（epoch を進める） | 実装済み（PLT-4632） | `ExecutionEnvironment::reassign` と `claim_for_reuse` の CAS |
| A1 / A6（N 個の **OS プロセス**で slot / idempotency key を奪い合う） | 未検証 → **PLT-4631 で実装**（下記） | 同じ file に別々の connection を持つ**スレッド**で alias CAS と claim を奪い合うテストだけがある（`repository/sqlite/tests.rs::cas_holds_across_separate_connections_to_the_same_file`）。プロセスを分けたテストは PLT-4631 |
| A2 / A3（lease の失効と回収、期限切れ renew の拒否） | 未着手 → **PLT-4631 で実装**（下記） | 上の決定 5 |
| A4（再起動後も Ready / Idle の環境が pool に残る） | 未着手（意図的） | 起動時の台帳 reconcile は P1 と同じく非 terminal の環境をすべて `Lost` にする。pool の環境は bridge session を失っており、再起動後に駆動できないため（`crates/application/tests/pipeline.rs::a_pooled_environment_is_reclaimed_after_a_restart`）。**このため同じ `data_dir` を複数の gateway が同時に開く構成は対象外**（後から開いた側が先の側の in-flight を `Lost` / `OutcomeUnknown` にする） |
| A5（reuse key の完全一致と index） | 実装済み（10k 行では未計測） | 8 field の完全一致は両 store の契約テスト、index 利用は `pool_lookups_use_their_indexes`（50 行 + `ANALYZE` の `EXPLAIN QUERY PLAN`） |
| A7（書き込み量が台帳サイズに比例しない） | 未検証 | 構造上は触れた行だけを書くが、書き込み量を測るテストは無い |
| A8（`state.json` の import） | 実装済み | `repository/sqlite/tests.rs::{a_p1_state_json_is_imported_once_and_moved_aside, an_interrupted_import_only_finishes_the_rename, a_state_json_next_to_a_populated_database_is_refused, corrupt_state_file_is_refused_with_a_hint, state_without_artifact_owners_still_loads}`、`apps/gateway/tests/gateway_integration.rs`（`state.json` を置いて起動する既存テスト） |
| A9（共通 suite） | 実装済み | `repository/contract_tests.rs` の 21 テストを `memory::*` と `sqlite::*` の両方で実行する。永続化に固有のテスト（roundtrip、restart、import、corrupt）は volatile 実装に意味が無いので SQLite のみ |

### TiDB（MySQL protocol）adapter にするときに変わるもの

**TiDB 互換は主張しない**（TiDB でも MySQL でも実行していない）。（2026-09-17 追記: 実 TiDB v8.5.8 で migration と契約テストを実行した。結果と、この節の予想との差分は「TiDB 検証（PLT-4618、2026-09-17）」。）SQL は移しやすい形に寄せた（`001_initial.sql` 冒頭の規則: `CREATE TABLE` / `CREATE [UNIQUE] INDEX` / `ALTER TABLE ADD COLUMN` だけ、key は長さ付き `VARCHAR`、partial index・`WITHOUT ROWID`・trigger・`ON CONFLICT` / `INSERT OR REPLACE` を使わず、upsert は「SELECT してから INSERT / UPDATE」を 1 トランザクションで行う、CAS 条件を `UPDATE ... WHERE` に置く）。それでも次は変わる。

1. **トランザクション**: `BEGIN IMMEDIATE`（DB 全体の書き込み lock）に相当するものは無い。正しさは `UPDATE ... WHERE` の CAS 条件と一意制約に依存させ、read-modify-write で読んだ行は `SELECT ... FOR UPDATE`（pessimistic）で押さえる必要がある。「存在確認してから INSERT」は一意制約違反を `Conflict` として扱う形に寄せる（`ArtifactOwnerRepository::claim` など）。
2. **DDL はトランザクションに入らない**: 1 migration に複数の DDL を書くと途中失敗で中途半端な schema が残る。1 DDL = 1 step とし、各 step を冪等（`IF NOT EXISTS`）にして `schema_version` を step 単位で進める。online DDL の制約（列追加の既定値、index 追加の backfill）も考える。
3. **型**: `body TEXT` は MySQL の `TEXT`（64 KiB）では base64 のインライン出力（既定上限 64 KiB → 約 87 KiB）が入らないので `MEDIUMTEXT` か `JSON`。timestamp は `DATETIME(6)`（または固定幅文字列のまま）。reuse key の版数は `BIGINT UNSIGNED`。`SMALLINT` の flag は `BOOLEAN`/`TINYINT`。
4. **主キー**: ULID は時刻順に増えるため TiDB では書き込みが末尾の region に集中する。clustered index を使わない（`NONCLUSTERED` + `SHARD_ROW_ID_BITS`）か、shard を足した複合キーにする。
5. **外部キー**: 使っていない（TiDB 6.6 未満は無視する）。親の存在と tenant の一致は `guard.rs` がトランザクション内で確認している。
6. **SQLite 固有の箇所**: `sqlite_master`（`migrations::current_version`）、`PRAGMA`、`EXPLAIN QUERY PLAN`、`ANALYZE`、`?N` placeholder の番号付き再利用（`(?8 IS NULL OR state = ?8)`）。前 2 つは `information_schema` と接続設定に、placeholder は位置引数に置き換える。
7. **API**: repository trait は同期で、invoke driver から同期に呼ばれる。ネットワーク越しの DB では blocking pool に逃がすか、trait を async にする（`crates/application` 全体に波及する。「比較」表の (3)）。
8. **reconcile**: 起動時に「非 terminal を全部 `Lost`」とする規則は owner の無い行だけに狭めた。owner のある行は dispatcher の lease に基づく回収（PLT-4631）で扱う。TiDB では `reclaim_expired` の 1 トランザクション（dispatcher 表の全件読み + 期限切れ lease の読み + 書き戻し）を、行ごとの CAS（`reclaimed_at IS NULL`、`released = 0`）を残したまま小さなトランザクションに割る必要がある。「環境 1 つにつき未 release の lease は 1 つ」は現在トランザクション内の確認（`BEGIN IMMEDIATE` が直列化する）で守っているので、`environments` に `active_lease_id` 列を置いて CAS に含めるか、部分一意 index の代わりの一意制約を用意する。
9. **時刻**: lease の期限は判定する側の wall clock と文字列比較（固定幅 RFC 3339）で評価している。control-plane を複数 host で共有する時点で、DB の時刻（`NOW(6)`）を基準にするか、skew の許容を host 間の NTP 精度に合わせて見直す。

## 実装メモ（PLT-4631、2026-09-17）

決定 1・5 と受入条件 A1・A2・A3・A6 を実装し、「同じ `data_dir` を複数の gateway が同時に開く」構成を対象に入れた。コードは `crates/application/src/repository/slot.rs`（port）、`repository/sqlite/slot.rs` と `repository/memory.rs`（実装）、`services/dispatcher.rs`、`services/reconcile.rs`、`services/invoke.rs`。schema は `003_slot_leases.sql`（expand のみ）。ADR が決めていなかった点は「選んだこと」に書いた。

### 入ったもの

| 項目 | 実装 |
|---|---|
| 決定 1（port の分割） | `SlotStore` を `EnvironmentRepository` から分けた。`EnvironmentRepository` は `insert` / `get` / `update` / `list_active` だけで、`update` は `Busy` への遷移・epoch の変更・owner と fencing の変更を拒否する（`guard::environment_update`）。pool（`list_idle` / `claim_for_reuse` / `release_to_pool` / `take_idle_for_termination`）、dispatcher（`register_dispatcher` / `heartbeat` / `stop_dispatcher` / `list_dispatchers`）、slot（`acquire` / `complete` / `release_lease` / `renew_lease` / `get_lease`）、回収（`reclaim_expired` / `list_fenced` / `confirm_terminated`）は `SlotStore`。`insert_lease` / `update_lease` は削除した（lease は acquire でしか作れず、complete / release / reclaim でしか閉じない）。`Repositories::slots` から使う |
| 決定 5（lease の期限を評価する） | lease は owner（`DispatcherId`）と所有期限 `expires_at` を持つ。取得: `acquire` が `now + lease_ttl_seconds`。renew: dispatcher の heartbeat が期限前の lease だけを延ばす（`ExecutionLease::renew` は期限後を拒否）。失効: `reclaim_expired` が `expires_at + max_clock_skew_ms` を過ぎた lease と、lease を失った dispatcher の lease を 1 回だけ release する |
| 決定 6（epoch） | 取得のたびに進める（`ExecutionEnvironment::assign`、0 → 1 → …）。fence でも 1 進める。pool の claim は `Idle` → `Ready` の予約で epoch を動かさない |
| slot の原子的取得 | `SlotStore::acquire`: 環境の `(state ∈ {Ready, Idle}, epoch)`、fenced でない、未 release の lease が無い、環境の owner = lease の owner、owner の dispatcher が live、Invocation が terminal でない、を 1 つの `BEGIN IMMEDIATE` で確認し、`UPDATE environments ... WHERE epoch = ? AND state = ?` の CAS の後に lease・attempt・invocation を書く |
| fencing | `SlotStore::complete`（遅れた callback を含む完了通知）は、未 release の同じ lease、同じ `(attempt_id, epoch)`、同じ epoch で fenced でない環境、terminal でない attempt / invocation を確認してから書く。どれかが違えば `Stale` で何も書かない |
| 失効と終了確認 | lease を失った dispatcher の環境は fence（`Draining`、epoch + 1、`fenced_at`、`fenced = 1`）。pool・acquire・capacity の対象外で、`confirm_terminated`（provider の terminate 成功後、同じ epoch の CAS）でだけ `Lost` になる。terminate に失敗したものは fenced のまま `list_fenced` から次の周期で再試行する |
| 再起動の台帳 reconcile | `SqliteStore::open` の P1 規則は owner の無い行だけに適用する。owner のある行は `Application::bootstrap` が dispatcher を登録した直後の `reclaim_ledger` で扱う |
| Idempotency-Key | 結び付けに `expires_at` 列（Invocation が terminal になった時点で `finished_at + [store] idempotency_retention_seconds`、実行中は NULL）。`lookup` と `insert_bound` は失効した結び付きを無いものとして扱い、`purge_expired_idempotency` が削除する（起動時と 10 分ごと）。一意性は従来どおり主キー。別 gateway が実行中の invocation への replay は台帳を追って待つ。409 は `AppError::IdempotencyConflict`（`invocation_id` と `Host.IdempotencyKeyReused`） |
| 受入条件 A1・A6（OS プロセス） | `repository/sqlite/tests.rs::separate_processes_racing_for_one_slot_or_one_key_have_one_winner`（テスト binary 自身を 6 プロセス起動し、全員の準備完了後に同時に acquire / bind）、スレッド版 `concurrent_acquires_on_separate_connections_have_one_winner_per_epoch`、`reclaim_and_key_binding_are_exactly_once_across_connections`、両 store の `contract_tests::acquire_is_a_cas_with_exactly_one_winner_per_epoch` |
| 受入条件 A2（OS プロセス） | `repository/sqlite/tests.rs::a_lease_left_by_an_exited_process_is_reclaimed_once_and_only_after_expiry`（子プロセスが lease を取って release せず exit。別 instance は期限前・skew 内では回収できず、以後 1 回だけ回収、遅れた完了は `Stale`。同じ instance の再起動は pid の不在で即回収） |
| 受入条件 A3 | `contract_tests::{leases_renew_only_while_unexpired_and_expire_past_the_clock_skew, a_fenced_dispatcher_can_neither_renew_nor_acquire}` |
| 2 つの gateway が同じ data_dir | `crates/application/tests/leases.rs::{two_gateways_on_one_data_dir_never_settle_each_others_work, a_key_replayed_on_another_gateway_returns_the_same_invocation_and_never_runs_twice, a_completion_delayed_past_a_reclaim_is_refused_and_the_slot_is_fenced, renewal_keeps_the_lease_and_a_graceful_stop_hands_over_at_once, a_fenced_environment_stays_fenced_until_its_terminate_succeeds}` |

### 選んだこと（ADR に書いていなかった点）

1. **lease の所有者は dispatcher（gateway プロセスの incarnation）**で、環境の owner は作った dispatcher から変わらない。bridge session がプロセス内にしか無いので、別 dispatcher が環境を引き継いで dispatch することはできない。他の dispatcher ができるのは fence と terminate だけ。
2. **renew は dispatcher 単位の heartbeat**（その dispatcher の全 lease を 1 トランザクションで延ばす）。driver ごとの renew は行わない。単体の `renew_lease` も port にある。
3. **時計のずれの許容**: 他者の期限は `expires_at + max_clock_skew_ms`（既定 2 s）を自分の時計で過ぎてから。
4. **期限を待たずに回収できる例外**: graceful shutdown で `stopped` になった dispatcher と、**同じ host 名・同じ instance 名**で pid が存在しない（または同じプロセス内で handle が drop 済みの）前の incarnation。A2 の「`expires_at` より前には回収できない」は、それ以外の dispatcher（別 instance）に対して成り立つ。instance の既定は `gateway@<listen>`。
5. **lease を失った dispatcher は自ら fenced になる**: heartbeat が拒否されたら新しい invoke を 503 で断り、`/readyz` を 503 にする。reclaim された dispatcher は acquire もできない。再登録（新しい id での復帰）はせず、再起動を運用に任せる。
6. **acquire は attempt と invocation `Running` まで同じトランザクション**に含めた（「slot を取ったのに attempt が無い」状態を作らない）。
7. **reclaim の分類**: dispatch 済み（lease あり）は `OutcomeUnknown`、dispatch 前は `Failed{platform_error}`。原因が期限切れなら `Host.LeaseExpired`、stopped / 前の incarnation なら `Host.Restarted`。自動再実行はしない。
8. **別 gateway が駆動中の invocation の cancel は 409**（ledger だけを `Cancelled` にすると、handler が走り続けたまま「止めた」と報告することになるため）。
9. **pool の上限**（`max_total_idle` / `max_idle_per_key`）は owner をまたいで数える（host 全体の上限）。

### 残るもの

| 項目 | 状態 | 内容 |
|---|---|---|
| A4（再起動後も Ready / Idle の環境が pool に残る） | 未着手（意図的） | 前の incarnation の pool の環境は fence → terminate → `Lost`（`crates/application/tests/pipeline.rs::a_pooled_environment_is_reclaimed_after_a_restart`）。session を失っているので駆動できない |
| A7 | 未検証 | PLT-4618 から変化なし |
| 2 つの gateway **プロセス**を HTTP で並べた E2E | 未検証 | 2 つの `Application` を 1 プロセスに置いた統合テストと、store を直接使う OS プロセスのテストだけ。`scripts/e2e/demo.sh` は gateway 1 つ |
| Firecracker / KVM 上での fence → terminate | 未検証 | fake provider での統合テストだけ |
| heartbeat の停止・時刻の飛び | 未検証（設計上の残存） | `max_clock_skew_ms` を超える時刻のずれや `lease_ttl + skew` を超える停止では生きている gateway の仕事が回収され、handler は terminate で止まる（台帳は fencing で守られる）。`docs/threat-model.md` §14-8 |
| `dispatchers` 表の retention | 未着手 | 起動ごとに 1 行増える |
| 複数 host | 非対象 | 本 ADR の「非対象」のまま |

## TiDB 検証（PLT-4618、2026-09-17）

製品の store は SQLite のまま変えない（`[store] backend` に `tidb` は足していない）。PLT-4618 の受入条件「実 TiDB 互換環境で migration と repository 統合テスト」を満たすために、**試験専用の TiDB adapter** を作り、実 TiDB で migration と契約テストを実行した。上の「TiDB（MySQL protocol）adapter にするときに変わるもの」はこの時点の予想で、実測との差分は下の「実測で分かったこと」にある。

### 環境と再現

| 項目 | 内容 |
|---|---|
| TiDB | v8.5.8 Community（PD / TiKV / TiDB 各 1、`Store: tikv`。unistore ではない）。binary は tiup mirror の `pd` / `tikv` / `tidb` v8.5.8 darwin-arm64（tiup が検証した署名付き manifest の sha256 と照合）。macOS 25.6 arm64、すべて 127.0.0.1 |
| 起動 | `scripts/db/tidb-verify.sh`。`tiup playground` v1.17.1 は自分の command server を `*:9527`、tidb-server の status port を `*:10080` で listen し、前者を loopback に限定する flag が無いため（試走で検出、script が失敗にした）、同じ binary を script が直接起動する。全 listener が loopback であることを `listeners.txt` で検査し、終了時に process が残っていないことを `processes-after-stop.txt` に記録する |
| client | `mysql` crate 28.0.2（`minimal-rust`、TLS なし）。dev-dependency だけで、gateway の binary には入らない |
| 証跡 | `docs/evidence/tidb-20260917T103330Z/`（`versions.txt`、`listeners.txt`、`tests.log`、`results.tsv`、`summary.txt`、`explain-tidb.md`、`explain-sqlite.md`、`processes-after-stop.txt`、各 server の log 末尾） |
| 結果 | 47 テスト中 47 pass（契約 30、object 契約 4、TiDB 固有 11、migration 検査 2。4 thread で 576 s、commit `bcf16ce`） |

MySQL では実行していない。TiDB 版 migration は `ADD COLUMN IF NOT EXISTS` / `ADD INDEX IF NOT EXISTS`（TiDB の拡張で MySQL 8.0 には無い）と `SHARD_ROW_ID_BITS` を使うので、MySQL で通っても TiDB 適合の証拠にはならず、逆もそのまま MySQL には流せない。

### 入ったもの

| 項目 | 実装 |
|---|---|
| migration | `crates/application/src/repository/tidb/migrations/001〜008`。SQLite の 001〜008 と同じ version・名前・表・列・index 名・主キー / unique key（`versions_mirror_the_sqlite_migrations`、`migrations_apply_to_an_empty_tidb_database` が表集合と全 index 名を SQLite と突き合わせる）。型は明示: id と比較する文字列は `utf8mb4_bin`、u64 の counter と reuse key の版数は `BIGINT UNSIGNED`、`body` は `LONGTEXT`、flag は `TINYINT`。timestamp は SQLite と同じ固定幅 RFC 3339 文字列 |
| migration の規則 | `tidb/migrations.rs` の doc。DDL は transactional でないので、全 statement を冪等（`IF NOT EXISTS`）かつ additive にし、`schema_version` は全 statement 成功後にだけ書く。migrator は `GET_LOCK` で 1 つ。expand → backfill（コード）→ 切り替え → 後の release で contract、同じ release に破壊的 DDL を入れない（`every_statement_is_idempotent_and_additive` が `DROP` 等を拒否）。詳細は `docs/db-index-review.md` §5 |
| adapter | `crates/application/src/repository/tidb/`（`#[cfg(test)]`）。`StateStore` 全体: Function / Revision / Alias（generation CAS）、Invocation / Attempt（terminal guard）、Environment、Log（memory）、Idempotency、ArtifactOwner、`SlotStore`（dispatcher、pool、acquire / renew / complete / release / reclaim / fence / confirm）、ConfigPublication、ObjectReference。行の不変条件は SQLite と同じ `guard.rs` |
| 契約テスト | `contract_tests.rs` と `object_contract_tests.rs` の `contract!` に `tidb` を追加（`TSLS_TIDB_URL` が無ければ理由を出して skip）。1 テスト 1 database（`tsls_t_<ulid>`、store の drop で削除） |
| TiDB 固有テスト | `tidb/tests.rs`: 空 DB / 古い DB（001〜003 に行を入れてから open）への適用、失敗した migration、binary より新しい schema の拒否、migrator の同時実行、別 connection pool 間の競合（acquire は 2/4/8/12 並列の各 round で勝者 1、alias CAS 8 並列で勝者 1、pool claim 8 並列で 1、expired lease の回収 8 並列で 1 回、同じ idempotency key の bind 8 並列で 1、revision 番号 8×5 並列で重複なし）、index review、READ-COMMITTED の lock 挙動の固定 |

### transaction の方式

- 接続ごとに `tidb_txn_mode = 'pessimistic'`、`transaction_isolation = 'READ-COMMITTED'`、`CLIENT_FOUND_ROWS`（affected rows = 条件に合った行数。同値更新で 0 にならない）。
- SQLite の `BEGIN IMMEDIATE` による全体直列化の代わりに、判断に使う行を `SELECT ... FOR UPDATE` で先に lock する（acquire: environment → dispatcher → invocation、complete: lease → environment、reclaim: 全 dispatcher → 未 release の lease）。CAS 条件は SQLite と同じく `UPDATE ... WHERE` に残し、0 行は負け。
- deadlock（1213）、lock wait timeout（1205）、write conflict（9007）、schema 変更（8028 / 8022）は transaction 全体を最大 12 回やり直す。closure は毎回読み直す。

### 実測で分かったこと（予想との差分）

1. **READ-COMMITTED の `FOR UPDATE` は、行の無い key を lock しない。** 別 session の同じ key の INSERT は待たずに成功する（3 ms）。REPEATABLE-READ では待つ（1 s で lock wait timeout）。`tidb_read_committed_does_not_lock_a_missing_key` が固定する。最初の実装はこれを前提にしていて、idempotency key の bind 競合で勝者が 1 を超えた（8 並列で 2〜3）。対処:
   - 「確認してから INSERT」は一意制約で決め、負けた側が相手の行を見る必要がある箇所（revision counter、idempotency binding）は重複キーで transaction 全体をやり直す。
   - 古い binding の削除は「invocation が無い / 期限切れ」の行だけにした。key 指定の DELETE は、確認の後に commit された他 transaction の生きた binding まで消していた。
   - 行の無い mutex（pool の上限判定、config の stamping）は `store_meta` の常在行（open 時に作る）を lock する。
   - object の attach と collection claim は、tombstone の key を**先に INSERT** して直列化する（未 commit の INSERT は key の lock を持つ）。attach は仮の `attaching` tombstone を入れて最後に消す。
   REPEATABLE-READ（TiDB の既定）にすれば行の無い key も lock されるが、lock 取得前の読みが transaction 開始時点の snapshot になり、lock 待ちの後に他者の commit が見えない。RC + 上の対処を選んだ。
2. **同じ `ALTER TABLE` で追加した列に index を張れない**（ERROR 1072 column does not exist）。002 / 003 は列の追加と index の追加を別 statement にした。SQLite と statement 数が違う。
3. 上の「変わるもの」3（`TEXT` の容量）は予想どおりで `LONGTEXT` にした。1 行は TiDB の `txn-entry-size-limit`（既定 6 MiB）を超えられないので、inline 出力の上限（`limits.max_response_bytes`）はこれ未満である必要がある。
4. 上の「変わるもの」4（ULID 主キーの hotspot）は `NONCLUSTERED` + `SHARD_ROW_ID_BITS` にしたが、主キー index 自体の偏りは残る（`docs/db-index-review.md` §3）。書き込み負荷での hotspot は計測していない。
5. 上の「変わるもの」8（reclaim を小さな transaction に割る）はしていない。1 transaction のまま、dispatcher 行の lock で直列化して契約テストを通した。回収対象が多いと大きな transaction になる。
6. index（10k invocation の seed）: 要求経路の query はすべて index / point get。TiDB が全件走査を選んだのは周期処理（pool sweep、outbox claim、due cron、inline 出力 / 送信済み outbox / trigger fire の retention）で、表が大きくなる構成では outbox claim と inline 出力 retention が先に効く（`docs/db-index-review.md` §2）。

### 残るもの（TiDB）

| 項目 | 状態 | 内容 |
|---|---|---|
| gateway からの利用 | 未着手（意図的） | adapter は `#[cfg(test)]`。gateway が要る `AsyncInvocationRepository`（outbox、PLT-4639）、`TriggerRepository`（PLT-4641）、`AsyncDispatchRepository`（PLT-4640）と、`SqliteStore` にだけある open 時処理（P1 `state.json` の import、owner の無い行の restart reconcile）を実装していない。schema（006〜008）は適用と index の確認まで |
| repository API の async 化 | 未着手 | adapter は同期の `mysql` crate。gateway に載せるなら blocking pool か async trait（上の「変わるもの」7） |
| TLS・認証 | 未着手 | 検証は loopback の root（password なし）だけ |
| 複数 TiDB node・障害注入 | 未検証 | 1 PD / 1 TiKV / 1 TiDB。region split、leader 移動、TiDB server の再起動中の transaction は試していない |
| hotspot・負荷 | 未検証 | 10k 行の EXPLAIN ANALYZE だけ。書き込み負荷での region 分布は計測していない |
| 時刻 | 未着手 | lease の期限は判定する側の wall clock（上の「変わるもの」9 のまま） |
| MySQL | 対象外 | 実行していない。TiDB 版 migration は TiDB 拡張を使う |
| budget の store（PLT-4643） | 対象外（この検証の時点） | `<data_dir>/usage/budget.db` は `state.db` の migration ではない別の SQLite（`crates/application/src/budget/store.rs`）で、TiDB 版を作っていない。`state.db` の migration は 008 まで mirror 済み（`versions_mirror_the_sqlite_migrations` が数の不一致で失敗する） |

## 参照

- 基準 RFC: quantum-box/knowledge PR #284「Tachyon Serverless 全体設計 RFC v0.1」（`docs/architecture.md` の先頭、`README.md` の「設計文書」）。**本リポジトリが節番号まで引用しているのは `crates/domain/src/environment.rs` の `ReuseKey`（RFC §5.3）だけ**である。本 ADR の「durable な control-plane と cell 局所 store の分割」は RFC §15 の形として PLT-4618 の指示で与えられたものを指しており、RFC 本文はこのリポジトリからは参照できない。RFC 本文と食い違う場合は RFC を正とし、本 ADR を改版する。
- コード: `crates/application/src/repository.rs`（8 trait / 34 method、`InMemoryStore`、`mutate`、`write_state`、`reconcile_after_restart`）、`crates/application/src/app.rs:120-131`（`with_persistence` の分岐と `Repositories::in_memory`）、`crates/application/src/services/invoke.rs`（write-through 12 箇所、lease と epoch）、`crates/domain/src/environment.rs`（`ExecutionEnvironment` / `ReuseKey` / `ExecutionLease`）、`apps/gateway/src/lib.rs:158`（終了時 flush）。
- 文書: `docs/architecture.md` §3・§5・§6、`docs/threat-model.md` §5・§6・§10・§14、`docs/inventory-tachyon-apps.md` §2・§3.1・§7、`docs/adr/0001-execution-provider-firecracker-first.md`（単一 host で完結する前提）、`docs/adr/0002-process-provider-dev-only.md`。
