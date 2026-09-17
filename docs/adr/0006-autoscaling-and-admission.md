# ADR-0006: 環境数は資源で予約する admission ledger・公平な bounded queue・起動 gate で決め、host の増設とは分ける

## ステータス

Accepted（2026-09-17、PLT-4634）。実装: `crates/application/src/services/admission/`（`state.rs` が状態機械、`mod.rs` が lock と待機者への配送、`scaler.rs` が token bucket・circuit breaker・需要推定、`resources.rs`、`config.rs`）。接続: `crates/application/src/services/invoke.rs`（driver）、`crates/application/src/services/pool.rs`（pool が予約を引き継ぐ）、`apps/gateway`（`GET /v1/capacity`）、`apps/cli`（`tsls capacity`、`functions deploy --region`）。

## コンテキスト

- P1 の容量制御は gateway 全体の semaphore（`[capacity] max_concurrency`）と revision ごとの semaphore（`execution.max_concurrency`）、bounded queue（`max_queue`、`queue_timeout_seconds`）だけだった。semaphore は「同時に走る invocation の数」を数えるだけで、次を表せない。
  - host の CPU / memory / disk。256 MiB の guest でも VMM と bridge の常駐分がある。
  - 起動中（`Starting`）の環境。起動に数百 ms〜秒かかる microVM で、起動中を数えなければ burst で上限を超えて起動する。
  - pool の idle 環境（PLT-4632/4633）。permit は invocation の終了で返るので、idle 環境が memory を持ったまま容量は空いて見える。
  - tenant 間の公平性。FIFO の queue では、1 tenant の大量の長時間 invoke が他 tenant を待たせる。
  - 起動失敗の暴走。壊れた revision は invoke のたびに microVM を起動しては失敗する。
  - 配置制約。`jp-only` の tenant をどの node に置いてよいかを表す場所が無い。
- PLT-4634 の受入条件: burst で必要数へ増え、起動中予約を重複加算して上限を突破しない。長短 2 tenant で starvation を防ぎ、queue の件数 / bytes / deadline を制限する。node 容量不足は待機 / 明示拒否へ収束し、jp-only を緩めない。VM / bridge の overhead を予約に含め、物理 host の増設と環境の増減を区別する。
- 前提: 1 gateway は 1 node（host）を持つ。環境は 1 invocation に 1 つ（`concurrency_per_environment = 1`）、`min_ready = 0`（PLT-4635 が zero-scale を持つ）。pool は既定 off。

## 選択肢

| 案 | 内容 | 採否 |
|---|---|---|
| A | semaphore を残し、permit 数を memory から逆算する | 不採用。revision ごとに資源が違うので permit 1 個の大きさが決まらない。pool の idle と起動中を表せない |
| B | 資源ベクトルで予約する ledger + 公平 queue + 起動 gate を 1 つの lock の下の同期状態機械にする（本 ADR） | 採用 |
| C | Kubernetes 風に、desired を別ループが計算して非同期に環境を増やす（reconcile 型 autoscaler） | 不採用。環境は invocation の driver が起動する設計（P1）で、所有者の無い先行起動は destroy-after-invoke・lease・fencing（PLT-4631）の前提を崩す。PLT-4635 の zero-scale / min_ready と一緒に再検討する |
| D | tenant ごとに固定の枠を割り当てる（hard partition） | 不採用。空いている枠を他 tenant が使えず、1 node の小さな容量では無駄が大きい。上限としての quota（下の決定 4）は併用する |

## 決定

1. **容量は資源で予約する（capacity ledger）。** 1 環境の予約 = revision の `resources`（cpu_millis、memory_mib、ephemeral_storage_mib）+ node の per-environment overhead（`[capacity.node] vmm_overhead_memory_mib`（既定 16）、`bridge_overhead_memory_mib`（既定 8）、`overhead_cpu_millis`、`overhead_storage_mib`）。node の容量は `[capacity.node] cpu_millis / memory_mib / ephemeral_storage_mib`（省略した次元は上限なし）。加えて `[capacity] max_concurrency` は node 全体の in-flight（`Promised` + `Starting` + `Busy`）の上限として残す。
2. **予約は値で、所有者は常に 1 人。** 予約（`Grant`）の状態は `Starting`（起動を許可した瞬間から）→ `Busy`（Ready）→ pool に渡すと `Parking`（quiesce 中）→ `Idle` → `Draining`（terminate 中）→ 解放。所有者は driver → pool → 次の driver と move し、最後の所有者が drop したときに解放される。terminate に失敗した環境は host に残っているので、pool が再試行して成功するまで予約を持ち続ける。warm 起動は pool の予約を引き継ぐ（`adopt`）ので新たに予約しない。重複加算が無いことは、全 counter を予約の集合から再計算して比べる property test で固定する（`services::admission::tests::reservations_are_counted_exactly_once_under_random_operations`、12 seed × 3000 操作）。
3. **拒否理由を明示する。** 受付時の即時拒否は台帳にも `Idempotency-Key` にも何も残さない（P1 と同じ）。

   | reason | いつ | HTTP |
   |---|---|---|
   | `queue_full` | 待ち行列の件数（`max_queue`）または payload bytes の合計（`max_queue_bytes`）を超える | 429 `capacity_exceeded` |
   | `quota` | tenant の待ち行列の持ち分（`max_queue`）を超える。または quota（tenant / revision の同時数）で待ったまま queue deadline | 429 / 504 `queue_timeout`（`Host.QuotaWaitTimeout`） |
   | `capacity` | 1 環境が node の容量を一度も満たせない。または node が満杯のまま queue deadline | 429 / 504（`Host.CapacityWaitTimeout`） |
   | `queue_deadline` | 順番・start token・起動の合流を待ったまま queue deadline | 504（`Host.QueueTimeout`） |
   | `circuit_open` | revision の起動失敗 breaker が open | 503 `provider_unavailable`（待機中の invocation は `Host.StartCircuitOpen`） |
   | `placement` | tenant / revision の `required_region` が node の `region` と一致しない | 503 `provider_unavailable` |

   エラー本文の `error.reason` に入る（`docs/api.md` §4）。
4. **公平な bounded queue。** tenant ごとの sub-queue。待機者のいる tenant を「in-flight 環境数 / weight」の小さい順（least attained service、同値は最後に割り当てられたのが古い順 = round robin）に見て、各 tenant の中は FIFO。長時間の invoke を多く持つ tenant は in-flight が大きいので後回しになり、短い invoke の tenant が空いた枠を先に取る。node 全体の制約（資源、`max_concurrency`、start rate）で止まった待機者より後ろの者には cold start を許さない（小さい要求の割り込みで大きい要求が飢えない。backfill しない）。quota・合流・probe で止まった待機者は他の tenant を止めない。
   - admission は **preemptive ではない**。長時間の tenant が先に全枠を取った後に来た tenant は、その invoke が終わるまで待つ。これを防ぐのは tenant の同時数 quota（`[capacity.tenant_defaults] max_concurrency` < `max_concurrency`、`[[capacity.tenants]]`）で、例の設定に書く。
5. **autoscaler は起動の gate として働く。** revision ごとに `desired = ceil((max(λ·W, in_flight) + backlog) / concurrency_per_environment)` を `[min_ready, min(execution.max_concurrency, tenant quota)]` に clamp する。λ は減衰する到着率（時定数 `[capacity.autoscaler] rate_window_seconds`）、W は handler 時間の移動平均、backlog は待機者数。Issue の式は `λ·W + in_flight + backlog` だが、λ·W（Little の法則）は in-flight の推定そのものなので足すと実行中を 2 回数える。大きい方を使う。
   - cold start は `Starting + Busy + Idle + Parking < desired` のときだけ許す（activation coalescing）。N 件の burst は `desired − (ready + starting)` 件しか起動せず、idle や quiesce 中の環境が使えるならそれを待つ（`activation_coalescing_counts_ready_and_parking_environments`）。
   - 起動は所有者（待機中の invocation）のいる分だけ行い、先行起動はしない。`min_ready` は prototype では 0 に固定（revision 検証）で、先行起動と zero-scale は PLT-4635。縮小は pool の idle TTL / drain と、下の eviction だけ。
   - **start-rate limiter**: node 全体の token bucket（`[capacity.start_rate] per_second`、`burst`）。warm 起動は消費しない。
   - **起動失敗 circuit breaker**: revision ごと。cold start の失敗（provider の create 失敗、handshake 失敗、init 失敗・init timeout）が `failure_threshold` 回続くと open。open の間は新しい invoke を即時拒否し、待機中の invoke も `circuit_open` で終える。`cooldown_seconds` 後に half-open になり 1 件だけ probe を通し、成功で closed、失敗で再び open。cancel・client deadline で probe が結果を出さなければ次の 1 件に probe を譲る。secret binding の解決失敗と artifact の不在は起動していないので数えない。**guest の init error（利用者のコード）も数える**（起動の暴走を止めるのが目的なので、原因がどちらでも同じ）。
6. **idle 環境は容量不足のときに追い出す。** cold start が node の資源で止まり、pool に idle 環境があるとき、admission は pool に最も古い idle 環境の terminate を頼む（`EnvironmentPool::evict_idle`）。同じ revision の idle があればそもそも warm で割り当てる。
7. **配置は緩めない。** `[capacity.node] region` が node の配置ラベル。`[[capacity.tenants]] required_region`（または `tenant_defaults`）と revision の `required_region`（`RevisionSpec.placement.region`、未指定なら serialize しないので既存 revision の digest は不変）の両方を満たす node でだけ admission する。ラベルが無い node はどの要求も満たさない。容量・負荷に関係なく、満たさなければ即時に `placement` で拒否する。
8. **物理 host と環境を分ける。** `GET /v1/capacity` と `tsls capacity` は node（名前・region・`hosts = 1`・`host_scale_out = "not_supported"`・設定した容量・overhead・`max_concurrency`）と、予約の合計・状態別の環境数・queue・start rate・拒否数・呼び出し元 tenant の revision（desired、状態別の数、到着率、平均時間、breaker）を別の欄で返す。**host の増設（node の追加、別 node への配置）はこの prototype の範囲外**で、admission が増減するのは 1 node の上の環境だけ。他 tenant の queue や revision は返さない。

## 結果（consequences）

- 既存の設定はそのまま動く。`[capacity.node]` を省略すると資源の上限は無く、`max_concurrency` と `max_queue` と revision の `max_concurrency` だけが効く（P1 と同じ挙動）。既定の per-environment overhead（24 MiB）は予約の数字に出るだけで、容量を設定しない限り何も拒否しない。
- queue deadline での失敗の `error_type` が理由で分かれる（`Host.CapacityWaitTimeout` / `Host.QuotaWaitTimeout` / `Host.QueueTimeout`）。class（`queue_timeout`、504）は変わらない。
- 同じ revision の init error が `failure_threshold`（既定 5）回続くと、`cooldown_seconds`（既定 30）の間その revision の invoke は 503 `circuit_open` になる。
- 状態は gateway プロセスのメモリにだけある。再起動で到着率・breaker・拒否数は消え、実行中の環境は起動時 reconcile（PLT-4631）が片付けるので予約の取り残しは無い。同じ `state.db` を 2 つの gateway で共有すると、それぞれが自分の分だけを数えるので node 全体の上限は守られない（1 host 1 gateway が前提）。
- 待機者の再評価は 25 ms 間隔の ticker（queue が空でない間だけ）と、予約の解放・到着のたびに行う。start token の補充はこの粒度で見える。
- overhead の既定値（VMM 16 + bridge 8 MiB）は推定で、実測ではない。`config/gateway.firecracker.toml` では PLT-4622 の VMM cgroup（`memory.max = guest memory + [provider.firecracker.cgroup] memory_overhead_mib`、既定 64）に揃えて `vmm_overhead_memory_mib = 64`、`bridge_overhead_memory_mib = 0`（bridge は guest memory の内側で動く）とした。これは host が強制する上限で数える保守的な値で、VMM の実 RSS の計測値ではない。process provider の bridge は host のプロセスなので既定の 8 MiB を足すが、これも計測していない。
- PLT-4636 の invoke gate（`docs/adr/0007`）との順序: invoke は `gate.resolve`（認可・設定の解決）の後に admission を受ける。cold start は `permit_cold_start` を予約の前に確認し（設定・認可・provider の理由で起動できない invocation は capacity の待ち行列に入らない）、待った後にもう一度確認する。gate の拒否は `config_unavailable` / `provider_unavailable` などの gate 自身の `error_type` で返り、admission の `reason` は付かず、breaker にも数えない。
- tachyon-apps の runner の容量計算との整合は確認していない（別リポジトリ、`docs/inventory-tachyon-apps.md` の範囲外）。

## 検証

- 決定的な fake clock のテスト（`crates/application/src/services/admission/tests.rs`、`scaler.rs`）: burst と上限、起動中予約の二重加算なし、資源（overhead 込み）の上限、start rate、合流、長短 2 tenant の 60 秒シミュレーション、queue の件数・bytes・tenant 持ち分・deadline、tenant / revision quota、breaker の遷移、jp-only、報告の分離、ランダム操作の property test。
- pipeline（fake provider、実時間、数秒）: `crates/application/tests/admission.rs`（burst、満杯 node の理由、jp-only、breaker、flood、pool と eviction）、`apps/gateway/tests/gateway_integration.rs::capacity_is_reported_per_tenant_and_a_burst_is_released`。
- 実 gateway + process provider の burst: `scripts/e2e/burst.sh`（`docs/evidence/*-burst-process/`）。
- KVM（Firecracker）上の burst は **未検証**（検証 VM を別作業が使用中のため実行していない）。
