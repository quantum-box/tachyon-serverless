# 既知の制約・国内有償 β との差分・次の設計判断（PLT-4650）

- 対象: Linear「Tachyon Serverless — 動作プロトタイプ」P0〜P4 と X1 の成果（本リポジトリ `main` `382d047` 時点）
- 基準: RFC「Tachyon Serverless 全体設計 RFC v0.1」（quantum-box/knowledge PR #284、`src/infrastructure/tachyon-serverless-architecture-rfc-20260911.md`。**設計提案・未 merge**）の §11.3・§12〜§19・§22・§23、本リポジトリの [acceptance.md](acceptance.md)、[adr/](adr/)、[benchmark.md](benchmark.md)、[failure-matrix.md](failure-matrix.md)、[runbook.md](runbook.md)、[threat-model.md](threat-model.md) §14
- 記録日: 2026-09-17
- 目的: **動くプロトタイプと、本番を任せられる公開サービスを混同しない。** 何を実際に測ったか、何がテストだけか、何が無いかを分け、国内有償 β（RFC §22 の M4）へ進む前に決めること・揃えることを並べる。

> [!IMPORTANT]
> ここにある数値はすべて **1 台の開発機（Apple M4 Mac）での 1 回ずつの記録**で、SLA・性能保証・販売価格の根拠ではない。Firecracker の記録は **Apple M4 上の Lima VM（aarch64、nested virtualization）だけ**で、x86_64 と bare metal では一度も動かしていない。process provider の記録は **隔離なし**で、microVM の証跡に使わない（[ADR-0002](adr/0002-process-provider-dev-only.md)）。fake provider のテストは実行基盤について何も測っていない（[ADR-0001](adr/0001-execution-provider-firecracker-first.md)「受入規則」）。

> [!NOTE]
> **「PLT-4649 実行後に更新」** の印は、プロトタイプ最終受入（PLT-4649: 同期・非同期・ゼロスケール・利用量を一気通貫で実証する、2026-09-17 時点 Backlog・未実行）の結果で値や状態が変わる箇所。**数値は推測で埋めていない。** 一覧は §9。

## 目次

1. 機能ごとの状態（実測済み / 実装済み・未実測 / 未対応）
2. RFC の仮定・仮目標と実測の差分
3. warm 対応 profile と queue の耐久条件
4. データ・秘密情報の所在地
5. 単一 host の故障範囲
6. 後続の設計判断
7. 国内有償 β へ進む条件（検証 / 契約 / 運用）
8. 今回実行していないこと
9. PLT-4649 実行後に更新する箇所

---

## 1. 機能ごとの状態

### 1.1 状態の区分

| 区分 | 意味 | 根拠にしてよいこと |
|---|---|---|
| **KVM 実測** | Firecracker v1.17.0 microVM（jailer・cgroup v2 required の設定を含む）で実行した記録が `docs/evidence/` にある。環境は Apple M4 → Lima VM（vz、nested virtualization、4 vCPU / 8 GiB、Linux aarch64）の 1 host だけ | 「この構成で動いた」まで。x86_64・bare metal・複数 host・長時間の挙動は言えない |
| **process 実測** | 実際の gateway・nats-server・TiDB などの OS プロセスを macOS arm64 の 1 host で動かした記録がある。関数は host の子プロセス（隔離なし） | 台帳・queue・usage・予算・API の挙動。**隔離・microVM の時間・資源は言えない** |
| **fake / 単体** | fake provider・偽 Firecracker・mock gateway・fake clock による自動テストだけ | ロジックの正しさ。実行基盤での挙動は言えない |
| **未対応** | 実装が無い、実験経路だけ、または対象外と決めた | — |

### 1.2 実行・隔離（Firecracker）

| 機能 | 区分 | 証跡 | 残り |
|---|---|---|---|
| P1 縦断（登録 → publish → invoke → logs → timeout → rollback → 他 tenant 404 → cancel → 破棄） | KVM 実測 / process 実測 | [evidence/20260915T171631Z-firecracker/](evidence/20260915T171631Z-firecracker/)（28/28）、[evidence/20260917T114908Z-firecracker/](evidence/20260917T114908Z-firecracker/)（28/28、jailer・cgroup required）、process は [evidence/20260917T110054Z-process/](evidence/20260917T110054Z-process/) ほか | x86_64・bare metal |
| host 強制 timeout・terminate・孤児 0 | KVM 実測（M5 は条件違い、M6 の 2 回目 terminate は未測定） | [evidence/kvm-20260915T080221Z/](evidence/kvm-20260915T080221Z/)、[evidence/bench-20260917T055450Z/](evidence/bench-20260917T055450Z/) `cleanup.txt`（714 request・370 環境の後に残留 0）、[evidence/kvm-final-chaos-20260917T152228Z/](evidence/kvm-final-chaos-20260917T152228Z/)（gateway SIGKILL 後に残った VMM を reclaim が jail・cgroup ごと回収、Running 中の VMM kill → `outcome_unknown`） | M6 |
| egress `none` / `restricted` / `public-web`、metadata・管理網・RFC1918・IPv6 の遮断、2 tenant 間遮断、起動前の policy 検証 | KVM 実測 | [evidence/isolation-20260916T020934Z/](evidence/isolation-20260916T020934Z/)、[evidence/isolation-20260917T011555Z/](evidence/isolation-20260917T011555Z/)、[evidence/isolation-20260917T031126Z/](evidence/isolation-20260917T031126Z/)、[ADR-0005](adr/0005-egress-profiles.md) | hostname allowlist、帯域上限、gateway kill 後の tap / nft 孤児 sweep の実機試験 |
| vCPU / memory / ephemeral storage 上限、VMM ごとの cgroup v2、jailer（uid 64000・CapEff 0・seccomp）、2 tenant の noisy neighbor | KVM 実測（1 回） | [evidence/isolation-20260917T041930Z/](evidence/isolation-20260917T041930Z/)（NOISY: 隣の CPU 部分 ×1.245、上限 ×1.30） | IO 帯域（`io.max`・drive `rate_limiter`）、環境ごとの uid、network namespace、複数回の分布 |
| warm 再利用（idle 休止・再開） | KVM 実測 | [evidence/warm-20260916T162532Z/](evidence/warm-20260916T162532Z/)（6 中 5 が warm、休止中 CPU tick 0）、[evidence/kvm-verify-plt4634-20260917T064519Z/](evidence/kvm-verify-plt4634-20260917T064519Z/)（warm 5/6） | 長時間 idle 後の再開、N ≥ 20 の再開分布、`[pool]` の既定は off |
| admission（burst・queue 上限・jp-only placement） | KVM 実測（burst 1 回）/ process 実測 / fake | [evidence/kvm-verify-plt4634-20260917T064519Z/](evidence/kvm-verify-plt4634-20260917T064519Z/)、[evidence/20260917T051238Z-burst-process/](evidence/20260917T051238Z-burst-process/)、[ADR-0006](adr/0006-autoscaling-and-admission.md) | 状態はプロセスのメモリだけ、1 host 1 gateway 前提、overhead は設定値（実測値ではない） |
| zero-scale・alias 切替 drain・削除 | KVM 実測（1 回）/ process 実測 / fake | [evidence/kvm-verify-plt4635-20260917T064940Z/](evidence/kvm-verify-plt4635-20260917T064940Z/)、[evidence/20260917T062958Z-zero-scale-process/](evidence/20260917T062958Z-zero-scale-process/)、[ADR-0009](adr/0009-scale-to-zero-and-drain.md) | `min_ready` 先行起動と cooldown は fake のみ |
| slot lease・fencing（同じ data_dir の 2 gateway） | KVM 実測（1 回）/ process 実測（故障マトリクス）/ fake | [evidence/kvm-verify-plt4631-fencing-20260917T070522Z/](evidence/kvm-verify-plt4631-fencing-20260917T070522Z/)、[ADR-0003](adr/0003-execution-state-persistence.md) | 実行中 attempt の lease 失効を FC で、時刻の飛び |
| cold / warm first response・同時実行・資源原価 | KVM 実測（2 回、共用 Mac。2 回目は最新 main、calibration で混雑を確認して開始） | [benchmark.md](benchmark.md) §6・§6.6、[evidence/bench-20260917T055450Z/](evidence/bench-20260917T055450Z/)、[evidence/bench-20260917T155921Z/](evidence/bench-20260917T155921Z/)（714 request 失敗 0、warm の基盤追加 p95 15 / 10 / 6 ms、cold p95 約 5.8〜6.1 s） | cold p95 ≤ 3 s 未達、gateway の常駐 RSS・idle CPU は main で増えた（19.5 → 30〜32 MiB、3 → 16〜18 tick / 60 s）、x86_64・bare metal・専用 host |
| Rust SDK の初期化保存点（X1） | KVM 実測（cold 経路のみ）/ process 実測 | [evidence/kvm-verify-plt4651-restore-aware-20260917T065206Z/](evidence/kvm-verify-plt4651-restore-aware-20260917T065206Z/)、[evidence/restore-aware-20260917T025043Z-process/](evidence/restore-aware-20260917T025043Z-process/) | restore 経路の hook は PLT-4653 の実験経路でだけ |
| snapshot / clone（X1、実験） | KVM 実測（実験経路、capability `Unverified`、既定無効） | [evidence/x1-restore-20260917T085700Z/](evidence/x1-restore-20260917T085700Z/)、[evidence/x1-clone-20260917T114231Z/](evidence/x1-clone-20260917T114231Z/)、[ADR-0015](adr/0015-snapshot-restore-feasibility.md)、[ADR-0017](adr/0017-snapshot-manifest-and-clone.md) | restore 後の secret 配送、egress 付き clone、別 host / 別 CPU、GC・容量上限。Cloud Hypervisor は検証 host で guest が handshake に届かず、Kata 経由は未対応 |
| public 公開 | **1 回だけ** ephemeral quick tunnel で公開（閉鎖済み） | [evidence/public-20260916T120349Z/](evidence/public-20260916T120349Z/)、[ADR-0004](adr/0004-public-ingress.md)（Proposed） | 安定した ingress は未実施。**第三者の海外 edge を通った記録で、国内完結の証拠ではない** |

### 1.3 durable・非同期・計測（process provider で実測。2026-09-17 の KVM 最終検証で Firecracker でも実行したものは区分に併記）

| 機能 | 区分 | 証跡 | 残り |
|---|---|---|---|
| durable queue（NATS JetStream 単一 node）・暗号化 object store | process 実測（kill -9 1 回） | [evidence/queue-objects-20260917T052554Z/](evidence/queue-objects-20260917T052554Z/)、[ADR-0008](adr/0008-durable-queue-and-object-store.md) | cluster・TLS・鍵 rotation・S3 互換・tenant ごとの queue 上限 |
| `invokeAsync` の永続受付・transactional outbox | process 実測（SIGKILL 4 回、nats 停止 1 回） | [evidence/async-e2e-20260917T064155Z/](evidence/async-e2e-20260917T064155Z/)、[ADR-0010](adr/0010-invoke-async-and-outbox.md) | 受付 throughput 未計測、複数 gateway の HTTP E2E |
| async dispatcher・retry・DLQ・redrive | KVM 実測（1 回）/ process 実測 | [evidence/kvm-final-async-triggers-20260917T150658Z/](evidence/kvm-final-async-triggers-20260917T150658Z/)（40/40、SIGKILL 2 窓・redrive・副作用は外部 store で各 1 回）、[evidence/async-dispatch-e2e-20260917T091034Z/](evidence/async-dispatch-e2e-20260917T091034Z/)、[ADR-0013](adr/0013-async-dispatch-retry-dlq.md) | DLQ・redrive 記録・inline 入力に保持期限なし。実行時間が ack wait を超えると実行中に 1 回再配送され duplicate として ACK される（ack wait 4 s の KVM 実行で観測） |
| cron・署名付き webhook | KVM 実測（1 回、SQLite queue）/ process 実測 | [evidence/kvm-final-async-triggers-20260917T150658Z/](evidence/kvm-final-async-triggers-20260917T150658Z/) `triggers/`（39/39）、[evidence/triggers-e2e-20260917T081400Z/](evidence/triggers-e2e-20260917T081400Z/)、[evidence/triggers-e2e-dispatch-20260917T091055Z/](evidence/triggers-e2e-dispatch-20260917T091055Z/)、[ADR-0014](adr/0014-cron-and-webhook-triggers.md) | source 別 webhook 検証、concurrency policy、data plane の trigger |
| 設定 cache・認可 lease・control plane 停止 | process 実測（2 プロセス）/ fake（provider 制御 API 停止） | [evidence/20260917T045347Z-split-process/](evidence/20260917T045347Z-split-process/)、[ADR-0007](adr/0007-config-distribution-and-auth-leases.md) | Firecracker 未実行、配信は平文 HTTP、`internal_token` の rotation 手順なし |
| host 由来 UsageEvent・ledger・仮料金 | KVM 実測（1 回）/ process 実測 | [evidence/kvm-final-usage-budget-20260917T151250Z/](evidence/kvm-final-usage-budget-20260917T151250Z/)（18/18、`EnvironmentStopped` に VMM cgroup の CPU usec・memory.peak が `provider_reported`、ledger = journal）、[evidence/usage-20260917T075537Z-process/](evidence/usage-20260917T075537Z-process/)、[ADR-0012](adr/0012-usage-ledger-and-rating.md) | 実請求は無効。`AttemptSettled` 単位の host CPU は無い（環境単位）。journal を含む durable store の fsync は warm invoke の host platform に約 7 ms（上限値、KVM の A/B 1 回） |
| 予算予約・hard / soft limit・fail closed | KVM 実測（1 回）/ process 実測 / fake（非同期 run の予約） | [evidence/kvm-final-usage-budget-20260917T151250Z/](evidence/kvm-final-usage-budget-20260917T151250Z/)（19/19）、[evidence/budget-20260917T102104Z-process/](evidence/budget-20260917T102104Z-process/)、[ADR-0016](adr/0016-budget-reservation-and-admission.md) | 予算 store の fsync が遅延に与える影響は未計測 |
| metrics・負荷シナリオ・detector | KVM 実測（5 シナリオ各 1 回、warm pool）/ process 実測 / fake（idle CPU を使う環境の検出） | [evidence/kvm-final-metrics-load-20260917T150242Z/](evidence/kvm-final-metrics-load-20260917T150242Z/)（cgroup の environment_stats、`warm_reuse`・`boot_changed` 0、休止中の idle CPU 0、findings 0）、[evidence/load-lifecycle-20260917T072540Z-process/](evidence/load-lifecycle-20260917T072540Z-process/) ほか `load-*`、[metrics.md](metrics.md) | promtool 未実行 |
| 故障マトリクス 20 シナリオ | process 実測（4 回、最終回 20/20）/ KVM 実測（Firecracker で挙動が変わる 6 シナリオ + guest OOM、3 回） | [failure-matrix.md](failure-matrix.md)、[evidence/chaos-20260917T115316Z/](evidence/chaos-20260917T115316Z/)、[evidence/kvm-final-chaos-20260917T152228Z/](evidence/kvm-final-chaos-20260917T152228Z/)（最終回 6/7、`stale_owner_sync_lease` は state.db lock で失敗） | provider 非依存の 13 シナリオは Firecracker 未実行。reclaim が終わらせた環境の host 原価は未計測。凍結した gateway が state.db の書込み lock を持つと同じ data_dir の他 gateway も止まる。host 喪失・disk 破損・電源断・partition は対象外 |
| 最小 console | process 実測（Playwright 15/15、OutcomeUnknown と loading 等は mock） | [evidence/console-20260917T110152Z/](evidence/console-20260917T110152Z/)、[console.md](console.md) | Tachyon Console 統合は設計のみ（[console-integration.md](console-integration.md)） |
| TiDB 版 migration・repository 契約 | 実 TiDB v8.5.8 で実測（試験専用 adapter、loopback 1 node 構成） | [evidence/tidb-20260917T103330Z/](evidence/tidb-20260917T103330Z/)、[db-index-review.md](db-index-review.md) | **製品の store は SQLite のまま**、outbox / trigger / dispatch の repository 未実装、TLS、複数 node、hotspot |
| fresh 環境からの lab runbook | process 実測・KVM 実測（どちらも **自動化 agent による** clean clone 追試） | [evidence/lab-20260917T1219Z-process-clean-clone/](evidence/lab-20260917T1219Z-process-clean-clone/)、[evidence/kvm-final-lab-firecracker-20260917T162937Z/](evidence/kvm-final-lab-firecracker-20260917T162937Z/)（firecracker: origin/main では `up` で止まり、lab.sh を 5 件直して demo all 49/49・teardown clean）、[runbook.md](runbook.md) | 別の人間による追試、x86_64、`LAB_FC_PRIVILEGED=0` |
| CI gate | hosted runner で実行確認、破損検出はローカル | [evidence/ci-gates-20260917T041348Z/](evidence/ci-gates-20260917T041348Z/)、[ci.md](ci.md) | **self-hosted KVM runner 未登録で `kvm` job は一度も実行されていない**、branch protection 未設定 |

### 1.4 fake provider / 単体テストだけのもの（実行基盤での記録なし）

| 項目 | テストの場所 | 何が足りないか |
|---|---|---|
| 2 回目の `terminate_environment` の冪等性（M6） | `crates/providers/firecracker/src/provider.rs` | 同上 |
| provider 制御 API 停止時に cold start だけ拒否 | `crates/application/tests/config_cache.rs` | E2E での再現 |
| `min_ready` 先行起動・scale-down cooldown の pool 挙動 | `crates/application/tests/scaling.rs` | Firecracker での記録 |
| idle CPU detector が CPU を使う idle 環境を検出する側 | `metrics::tests`、`scaling.rs` | Firecracker での記録（検出 0 の側と boot identity は KVM 実測済み） |
| 他 tenant snapshot の拒否 | `crates/domain/src/snapshot_tests.rs`、`tests/restore.rs` | 2 tenant を並べた KVM run |
| client 切断後の deadline までの追跡 | テストなし（コードのみ） | テスト自体 |
| 非同期 run の予算予約 | `invoke_async::dispatch_tests` | E2E |

### 1.5 未対応（実装が無い・対象外）

| 項目 | 状態 | 参照 |
|---|---|---|
| x86_64 KVM host、bare metal | 未実行 | [acceptance.md](acceptance.md)「横断」 |
| 複数 host・HA（gateway / store / queue / object の冗長化） | 対象外 | [failure-matrix.md](failure-matrix.md) §9、[ADR-0003](adr/0003-execution-state-persistence.md)「非対象」、[ADR-0008](adr/0008-durable-queue-and-object-store.md) §6 |
| backup / restore / PITR | 無い | [ADR-0003](adr/0003-execution-state-persistence.md)「非対象」、[ADR-0008](adr/0008-durable-queue-and-object-store.md) §6 |
| KMS・鍵 rotation・保存時暗号化（`state.db`） | 無い | [threat-model.md](threat-model.md) §14-4・§14-11・§14-19 |
| 実請求・決済・請求書・訂正 | 無効（設定で有効化できない） | [ADR-0012](adr/0012-usage-ledger-and-rating.md)、[ADR-0016](adr/0016-budget-reservation-and-admission.md) |
| 安定した public ingress、TLS 終端、内部通信の相互認証 | 無い（ADR-0004 は Proposed） | [ADR-0004](adr/0004-public-ingress.md) |
| request rate limit、abuse 対応手順、WAF / DDoS 対策 | 無い | [ADR-0004](adr/0004-public-ingress.md)「demo より長く上げる前に必要なもの」、[threat-model.md](threat-model.md) §15 |
| log の検索・転送（OTel）・保存時暗号化 | 無い。invocation log の永続化と保持期限・総量上限は実装済み（`<data_dir>/logs/logs.db`、process の E2E と unit / integration テストで確認、Firecracker の restart step も [evidence/kvm-final-logs-restart-20260917T145357Z/](evidence/kvm-final-logs-restart-20260917T145357Z/) で通過） | [ADR-0018](adr/0018-durable-invocation-logs.md)、[architecture.md](architecture.md) §4「invocation log」 |
| OCI image の pull・実行 | 無い（参照を受理して理由付き `Failed`） | [acceptance.md](acceptance.md) PLT-4620 |
| Kata / Cloud Hypervisor adapter、Knative との比較 | 無い | [ADR-0001](adr/0001-execution-provider-firecracker-first.md)、[benchmark.md](benchmark.md) §6.5 |
| tachyon-apps の secret backend・利用者認証との接続 | 無い | [acceptance.md](acceptance.md) PLT-4623 #6、[console-integration.md](console-integration.md) §3.1 |

---

## 2. RFC の仮定・仮目標と実測の差分

RFC §19 の数値は RFC 自身が「設計用の仮目標であり、達成済み値・公約ではない」としている。下の判定は **この 1 host（aarch64 nested virtualization の共用 Mac）での判定**で、bare metal での達成・未達を意味しない。

### 2.1 性能・信頼性の仮目標（RFC §19）

| 仮目標（RFC） | 実測値 | 判定 | 理由・条件 |
|---|---|---|---|
| cold first response（小さな Rust 関数、image cache hit）p95 ≤ 3 s | hello client p95 **6037 ms**（n=20、失敗 0）。http-axum 9430 ms、cpu-burn 12250 ms。boot p95 5315 ms（hello） | **未達** | boot 中ずっと 500 m の cgroup quota に当たる（throttled period 比 p50 0.99）。nested virtualization の serial console が重い。bare metal・console 抑制・quota 変更の値が無いので、構造的に 3 s を超えるかは判断できない（[benchmark.md](benchmark.md) §6.4） |
| cache miss を別集計 | fresh host p50 / p95: hello 6158 / 7609、http-axum 5908 / 6128、cpu-burn 6925 / 14106 ms（n=5） | 記録のみ | RFC に数値目標なし。macOS 側の cache は落とせず「新品 host」ではない |
| warm の基盤追加遅延 p95 ≤ 20 ms（handler 除く） | host 計測 total − handler の p95: hello **17**、cpu-burn **17**、http-axum **31** ms | **一部未達** | 逐次・loopback・1 host。RFC の「同一 region・所定負荷」条件ではない。http-axum の揺れの原因は未調査 |
| Fast Restore が cold より end-to-end p95/p99 と原価で改善 | X1 実験経路の逐次 restore 3 回: `total_ms` 1355 / 1097 / 1134 ms（verify 0.7〜0.9 s を含む）、同条件の cold 2856〜3221 ms。sample は `examples/restore-aware` | **未測定**（参考値のみ） | n=3 で p95 / p99 が無い、原価（封印 342 MiB・verify の CPU）を測っていない、`scripts/kvm/bench.sh` の同じ方法に載せていない、capability `Unverified`（[ADR-0017](adr/0017-snapshot-manifest-and-clone.md)「結果（KVM）」） |
| 正常 invoke の可用性 99.9% 相当（β で観測） | 逐次 group の失敗率 0、並列 8 で 8〜17%（queue timeout と boot 失敗）。**PLT-4649 実行後に更新**（最終受入の失敗率） | **未測定** | 数百 request・1 host は可用性の測定ではない。測定窓・除外条件の定義も無い |
| durable async 受付: 単一 node 故障の範囲で ACK 済み受付の消失 0 | 故障マトリクス 20 シナリオ × 4 回で `cv.accepted_never_lost` 全 pass、async-e2e で受付 24・欠落 0。**PLT-4649 実行後に更新**（最終受入での件数） | **一部達成**（プロセス障害の範囲） | 試したのは gateway / nats-server / bridge の kill・停止と store の lock。**node（host）の喪失・disk 喪失・電源断は未試験**。すべて同じ host に載るので node 喪失では台帳ごと失う（§5） |
| backup 復旧: 別の国内故障ドメインへの復旧で RPO / RTO | backup が存在しない | **未測定** | backup・PITR・別故障ドメインが無い |
| 二重課金 0（同一 UsageEvent の重複計上 0） | property test、collector crash の replay、全 chaos シナリオの `cv.usage_counted_once` が pass。**PLT-4649 実行後に更新** | **達成**（単一 host・process provider の範囲） | ledger は local SQLite で複製・署名なし。Firecracker での usage 計測は未実行。dispatch 済みで gateway が kill -9 された attempt は利用量に出ない（少なく数える側） |
| M2: 無負荷でゼロ、無料 idle で user CPU が走らない | Firecracker の休止環境 60 s で VMM tick 0・cgroup `usage_usec` 増分 0（3 sample）。zero-scale を FC で 1 回 | **達成**（aarch64 nested 1 host） | 休止は memory を返さない（1 環境 約 41 MiB、disk 約 268 MiB を保持）。「環境 0 = host 費用 0」ではない |

### 2.2 構成・方針の仮定（RFC §0・§6・§11・§13〜§17）

| RFC の仮定 | プロトタイプの実際 | 差分の種類 | 根拠 |
|---|---|---|---|
| §0・§6: **Kata + Cloud Hypervisor を維持**し、`KataPodProvider` で Kubernetes に配置 | **Firecracker を直接**操作する provider、Kubernetes なし（`ExecutionProvider` trait の背後） | 方針の変更（プロトタイプ内の決定） | [ADR-0001](adr/0001-execution-provider-firecracker-first.md)。Kata は bridge の PID 1・vsock 規約と合わず、稼働 sandbox の checkpoint も未対応（[ADR-0015](adr/0015-snapshot-restore-feasibility.md)）。**RFC 本体は改版していない**（§6 の判断事項） |
| §6.4: Knative と同じ Kata・同じ image・同じ負荷で比較 | 実行していない | 未測定 | [benchmark.md](benchmark.md) §6.5 |
| §7.3: architecture `x86_64` | aarch64 だけで実測 | 未測定 | [acceptance.md](acceptance.md)「横断」 |
| §11.3: memory 512 MiB〜4 GiB、CPU 0.25〜2、入力 1 MiB、出力 6 MiB、実行 15 分 | memory 128〜4096 MiB、CPU 250〜2000 m、入力 1 MiB、出力 6 MiB、timeout 1〜900 s、1 環境 1 同時実行 | 概ね一致（memory の下限が低い） | [threat-model.md](threat-model.md) §11 |
| §11.3: guest OS・VMM・bridge の memory を実測値で予約 | 予約に含めるが値は設定値（64 MiB）。実測は VMM RSS 約 38.7 MiB・cgroup 約 41 MiB | 一部（実測値を設定に反映していない） | [benchmark.md](benchmark.md) §6.3、[acceptance.md](acceptance.md) PLT-4634 #4 |
| §12.4: 無料 warm 待機は `idle_quiesce` / `idle_resume` の合格後 | Firecracker は実機計測を経て `Supported`、ただし `[pool]` の既定は off | 一致（条件付き） | [ADR-0001](adr/0001-execution-provider-firecracker-first.md) 決定 5 |
| §13.5: 同一 host・同一 CPU → 同一 profile の別 host の順に clone を検証 | 同一 host・同一 CPU だけ | 一部 | [ADR-0017](adr/0017-snapshot-manifest-and-clone.md) |
| §14.1: node・controller・gateway 間は相互認証、role と cell を制限 | control plane ↔ data plane は平文 HTTP + `internal_token`、queue は loopback 平文 + password | 未達 | [ADR-0007](adr/0007-config-distribution-and-auth-leases.md)、[ADR-0008](adr/0008-durable-queue-and-object-store.md) §3 |
| §14.3: secret は broker から割当先環境だけへ短期権限で配送 | binding の値は gateway 設定ファイル（TOML）に平文。配送は `HelloAck.env` だけ | 差分 | [threat-model.md](threat-model.md) §4 A2 |
| §15: 管理データは既存 TiDB を拡張、async ledger は国内配置を確認した TiDB | 埋め込み SQLite（`state.db`）。TiDB は試験専用 adapter で契約テストだけ | 差分 | [ADR-0003](adr/0003-execution-state-persistence.md)「TiDB 検証」 |
| §15: queue は国内の JetStream | JetStream 単一 node、local disk、複製なし | 一部一致（冗長化・国内配置は未） | [ADR-0008](adr/0008-durable-queue-and-object-store.md) §6 |
| §15: image・入出力・snapshot は国内 S3 互換 object store | gateway host の local directory（AES-256-GCM、単一鍵） | 差分 | 同上 |
| §15: logs / metrics / traces は国内 OTel pipeline | invocation log は同じ host の `<data_dir>/logs/logs.db`（再起動で残る、転送なし）、gateway log は stdout、metrics は `/metrics`（メモリ） | 差分 | [architecture.md](architecture.md) §4、[metrics.md](metrics.md) |
| §15: 保持（一般ログ 7 日、metadata 30 日、正常終了の入出力 24 時間、DLQ 7 日） | inline 出力は既定 7 日で digest に置換、invocation metadata は保持期限なし、DLQ・redrive 記録・inline 入力は保持期限なし、queue `max_age` 7 日、object TTL 7 日、invocation log は最後の行から既定 7 日（`[logs] retention_seconds`）と総量上限 1 GiB（実行中の invocation は消さない） | 未達 | [ADR-0003](adr/0003-execution-state-persistence.md)、[acceptance.md](acceptance.md) PLT-4640「残り・制約」 |
| §16.1: UsageEvent は host / 入口の journal → durable stream → ledger | host の SQLite journal → 同じ host の SQLite ledger（stream なし、複製・署名なし） | 一部 | [ADR-0012](adr/0012-usage-ledger-and-rating.md) |
| §16.4: hard budget は最大額の credit を事前予約し終了時に精算 | 実装済み（process 実測）。1 `data_dir` の中だけ、複数 cell の予算 token 配分は無い | 一部 | [ADR-0016](adr/0016-budget-reservation-and-admission.md) |
| §17.1: strict profile では管理 DB・queue・image・snapshot・secret・logs・backup・crash dump・TLS 終端の拠点を確認 | 全部が 1 台の開発機の上。唯一の公開は Cloudflare quick tunnel（第三者 edge で TLS 終端） | 未達 | §4、[ADR-0004](adr/0004-public-ingress.md)「residency についての正確な言い方」 |
| §17.3: 有償 β は複数 worker、入口冗長化、3 投票ノードの queue / control plane、冗長 storage、別の国内故障ドメインへの backup、N+1 | すべて無い（1 host・1 gateway・1 nats-server・1 file） | 未達 | [failure-matrix.md](failure-matrix.md) §9 |
| §20.2: 実 Kata runner で VM 境界等を変更範囲別に検証 | workflow と gate はあるが self-hosted KVM runner 未登録 | 未達 | [ci.md](ci.md) |
| §22 M1: Hello World・Webhook・DB を使う Rust API・長めの計算の 3 種を基準 | hello / http-axum / cpu-burn（DB を使う sample は無い） | 一部 | [acceptance.md](acceptance.md) PLT-4630 |

---

## 3. warm 対応 profile と queue の耐久条件

### 3.1 provider ごとの能力（`Capabilities`）

| provider / 構成 | `idle_quiesce` / `idle_resume` | 既定の再利用 | `snapshot_create` / `snapshot_clone` | 証跡・注記 |
|---|---|---|---|---|
| Firecracker（jailer・cgroup required、`profile = "production"`） | **Supported** | off（`[pool] enabled = false`）。有効時だけ warm | **Unverified**（feature `experimental-restore` 付き build・jailer ありの場合。`[snapshots] allow_unverified` が無ければ拒否）。feature なし build・jailer なしは理由付き `Unsupported` | [evidence/warm-20260916T162532Z/](evidence/warm-20260916T162532Z/)（cold `total_ms` 中央値 14663 / warm 230、`resume_ms`・`readiness_ms` 中央値 9、休止中 3 s で CPU tick 0・RSS 38 MiB 据え置き）、[evidence/bench-20260917T055450Z/](evidence/bench-20260917T055450Z/)（warm n=20 × 3 sample、休止 60 s の CPU 0）、X1 は [evidence/x1-clone-20260917T114231Z/](evidence/x1-clone-20260917T114231Z/)。いずれも aarch64 nested 1 host |
| process（開発専用、隔離なし） | **Unsupported** | 常に destroy-after-invoke（`reuse.mode = every_invocation_boots`） | Unsupported | `production` profile では起動を拒否（[ADR-0002](adr/0002-process-provider-dev-only.md)）。`min_ready` は満たされず `scaling.warm_pool = false` |
| fake（テスト専用） | テストで `warm_capable` を宣言できる | — | テストで宣言 | pool・scaling・restore のロジック検証用。実行基盤の証拠にしない |
| Kata / Cloud Hypervisor | adapter なし | — | Kata 経由の snapshot / clone は **未対応**（VM template は関数状態を含まない、稼働 sandbox の checkpoint は無い）。CH は検証 host で guest が handshake に届かず **未成立** | [ADR-0015](adr/0015-snapshot-restore-feasibility.md) |

注意:

- warm 再利用の条件は「provider が両方を `Supported` と申告」**かつ**「`[pool] enabled = true`」。どちらかが欠ければ destroy-after-invoke で、warm 成功とは報告しない（[acceptance.md](acceptance.md) PLT-4632）。
- 休止は memory を返さない（1 環境 約 41 MiB を host が保持）。idle 環境 1 つあたりの disk は約 268 MiB。
- restore（X1）は `restore.policy = prefer` なら互換 snapshot が無いとき cold に落ち、`start_kind = cold` と `restore_fallback` で記録する。cold を restored と数えない。secret binding・egress `restricted` / `public-web` のある revision は対象外。

### 3.2 queue（非同期）の耐久条件

| 条件 | 値・挙動 | 根拠 |
|---|---|---|
| 構成 | NATS JetStream **単一 node**（nats-server v2.14.7 を 1 process）、file store、`num_replicas = 1`、cluster なし、listen `127.0.0.1`、TLS なし | [ADR-0008](adr/0008-durable-queue-and-object-store.md) §2・§3・§6 |
| 永続化の粒度 | `sync_interval: always`（publish の ACK 前に fsync）。throughput への影響は未計測 | 同 §3・「結果」 |
| stream | `workqueue` retention、`discard: new`（満杯なら新しい publish を `queue_full` で拒否し既存を消さない）、既定上限 100 000 件 / 256 MiB / 1 message 256 KiB / `max_age` 7 日 | 同 §2、[threat-model.md](threat-model.md) §11 |
| 配送の意味 | **at-least-once**。ACK は台帳の terminal commit の後。commit と ACK の間の crash は再配送され、台帳の CAS / terminal 記録で二重実行を止める | [ADR-0008](adr/0008-durable-queue-and-object-store.md) §1、[ADR-0013](adr/0013-async-dispatch-retry-dlq.md) §10 |
| 再配送回数 | **crash をまたぐと `delivery_count` が 0 に戻る**（Linux の CI で観測。macOS では保持）。`max_deliver` を再試行上限にしない。試行回数と DLQ は台帳の attempt 記録で判定 | [ADR-0008](adr/0008-durable-queue-and-object-store.md)「追記: クラッシュ後の配送回数」 |
| publisher の dedup | `Nats-Msg-Id` + `duplicate_window`（既定 120 s）。**kill -9 の後は stream に残っている message からしか dedup 表を作り直さない**ので、ACK 済み（削除済み）の id の再 publish は新規として受理される。exactly-once の根拠にしない。window 外の再 publish は 2 通になるが同じ invocation に解決される | [ADR-0008](adr/0008-durable-queue-and-object-store.md) §1、[acceptance.md](acceptance.md) PLT-4639 #2b |
| 受付（outbox）の保証 | 入力・Invocation・Idempotency-Key・object 参照・outbox event を **1 トランザクションで COMMIT した後にだけ 202**。request の中では publish しない。publisher は claim → publish → mark。COMMIT 後 / publish 前 / publish 後 mark 前のどの crash でも同じ invocation に収束（process 実測） | [ADR-0010](adr/0010-invoke-async-and-outbox.md) |
| queue 停止時 | outbox の上限（既定 `max_pending_events = 10000`）までは 202 で受け、以後 503 `queue_unavailable`。同期 invoke は影響なし。再開後に outbox が空になり全件 terminal（`broker_sigstop` / `broker_sigkill` で実測） | [failure-matrix.md](failure-matrix.md) §4・§5 |
| broker が event を失った場合 | reaper が「未 publish なし・`stall_timeout_seconds` 経過・backlog 0」を検出して次の generation を publish する設計（**JetStream store の喪失そのものは未試験**） | [ADR-0013](adr/0013-async-dispatch-retry-dlq.md) §9 |
| 外部副作用 | handler の副作用の後・結果 commit の前の crash では **もう一度実行される**（実測）。利用者の業務冪等キーが前提 | [ADR-0013](adr/0013-async-dispatch-retry-dlq.md) §10、`examples/idempotent-async` |
| 失われるもの（disk / host 喪失） | JetStream の未処理 event、object、**そして同じ host の `state.db`（outbox・invocation・idempotency・DLQ）そのもの**。複製・backup なし。process crash / kill -9 では失わない（実測） | [ADR-0008](adr/0008-durable-queue-and-object-store.md) §6、§5 |
| tenant 公平性 | queue の上限は stream 全体で tenant ごとの持ち分なし。1 tenant が満杯にすれば他 tenant も `queue_full`。outbox 上限も全 tenant 共通 | [threat-model.md](threat-model.md) §14-11・§14-13 |
| SQLite queue | `<data_dir>/queue.db` の埋め込み実装は開発・CI 用。`profile = "production"` では拒否 | [ADR-0008](adr/0008-durable-queue-and-object-store.md) §2 |

---

## 4. データ・秘密情報の所在地

**すべて 1 台の local host（開発機の Apple M4 Mac、Firecracker の場合はその上の Lima VM）の local disk またはメモリにある。複製は無い。backup は無い。** 所在地を国・事業者・データセンターの単位で説明できる構成ではない。

| データ | 実際の置き場所 | 暗号化・権限 | 複製 / backup | 保持 |
|---|---|---|---|---|
| 関数の実行（microVM / process） | gateway と同じ host（Firecracker: Lima VM 内の KVM、process: macOS の子プロセス） | Firecracker は jailer chroot・cgroup、process は隔離なし | — | 環境ごと（pool 有効時は idle TTL まで） |
| artifact（実行バイナリ） | `<data_dir>/artifacts/`（digest 名） | 暗号化なし（file mode 0755） | なし | 削除の仕組みなし |
| 台帳 `state.db`（function / revision / alias / invocation / attempt / environment / lease / idempotency / outbox / trigger / dead letter / object 参照） | `<data_dir>/state.db`（+ `-wal` / `-shm`） | **保存時暗号化なし**、新規作成時 0600。inline 出力・入力 digest・idempotency key を含む（secret 値は含まない）。webhook secret は AES-256-GCM で封じて格納 | なし | inline 出力は既定 7 日で digest に置換。それ以外は増え続ける |
| usage journal / ledger | `<data_dir>/usage/journal.db`、`<data_dir>/usage/ledger.db` | 暗号化なし。journal は 0600・hash chain（root は chain ごと書き換え可能） | なし | 定義なし |
| 予算台帳 | `<data_dir>/usage/budget.db` | 暗号化なし、複製・署名なし | なし | 定義なし |
| queue（JetStream） | nats-server を動かす同じ host の local disk（lab は `<lab>/nats/jetstream`、既定 `target/queue/nats/jetstream`） | 暗号化なし、loopback 平文 | なし（`num_replicas = 1`） | `max_age` 7 日、ACK で削除 |
| object（大きな入出力） | `<data_dir>/objects/<region>/<tenant>/` | AES-256-GCM（全 tenant 共通の 1 鍵、rotation なし）、digest 照合 | なし | TTL 既定 7 日、非 terminal の参照があれば保持 |
| snapshot（X1、既定無効） | 封印: `<data_dir>/snapshots/`、平文: provider の `<workdir>/_snapshots/`（root 専用） | 封印側は AES-256-GCM chunk + 署名、平文 cache は host に残る | なし | GC・容量上限なし |
| invocation log | `<data_dir>/logs/logs.db`（+ `-wal` / `-shm`。`[store] backend = "memory"` のときだけ gateway のメモリ）。invocation ごと 2000 行 / 1 MiB | **保存時暗号化なし**、directory 0700・file 0600。user code の出力を平文で含む（host は secret 値を書かない） | なし | 最後の行から既定 7 日、保存量 1 GiB 超で terminal の古い invocation から削除。crash で失うのは直近 1 flush 間隔（既定 200 ms）の行まで（[ADR-0018](adr/0018-durable-invocation-logs.md)） |
| gateway の log | stdout / stderr（lab は `<lab>/logs/gateway.log`）、Firecracker の `console.log` / `fc.log` は provider workdir（各 4 MiB 上限） | 平文 | なし | rotation・保持なし |
| metrics・admission・scale の状態 | gateway プロセスのメモリ | — | なし | 再起動で 0 |
| backup | **無い** | — | — | — |
| API token・secret binding の値 | gateway 設定ファイル（`config/gateway.*.toml`、lab は `<lab>/config/gateway.toml` 0600）に平文 | file 権限だけ | なし | 失効は設定変更 + 再起動 |
| 鍵（object 鍵、trigger 封印鍵、snapshot 暗号鍵・署名鍵）、nats password、metrics token、`internal_token` | host 上の file（64 hex、**0600 必須**）または環境変数。lab は `<lab>/secrets/`（0700）と `<lab>/nats/gateway.password` | **KMS なし・HSM なし**、rotation 手順なし（trigger secret の rotate は猶予期間なし） | なし | 鍵を失えば該当 object / snapshot / webhook secret は読めない |

### 4.1 region と国内性についての言い方

- `[capacity.node] region = "jp"` や revision の `required_region = "jp"` は **scheduling label** で、`jp` 以外の node に載せないことを強制するだけ（[runbook.md](runbook.md) §3、[acceptance.md](acceptance.md) PLT-4634 #3）。**データがどの国のどの disk にあるかの証明ではない。**
- object store の `regions` も「置いてはいけない場所へ黙って置かない」ための境界で、複製先や地理的な保管保証ではない（[ADR-0008](adr/0008-durable-queue-and-object-store.md) §6）。
- 検証に使った host は個人の開発機で、所在地・電源・回線・物理アクセスは管理された条件ではない。**海外の検証 pool（RFC §17.2 の Hetzner など）や、ここでの記録を国内完結の証拠にしない。**
- RFC §15 と同じく、location hint（例: object storage の location hint）や製品名（TiDB、JetStream）だけで国内完結と判断しない。strict な国内 profile を名乗るには §7 の契約・構成の証跡が要る。
- 唯一の public 公開（[evidence/public-20260916T120349Z/](evidence/public-20260916T120349Z/)）は Cloudflare quick tunnel で、**request / response の平文が第三者の edge を通った**。これは transport の residency を満たさない例として扱う（[ADR-0004](adr/0004-public-ingress.md)）。

---

## 5. 単一 host の故障範囲

前提: gateway 1 process（`combined`）、`state.db` 1 file、nats-server 1 process、object root 1 directory、usage / budget の SQLite、provider がすべて同じ host。故障マトリクスは **process provider・macOS arm64** での記録で、Firecracker では未実行（違いは [failure-matrix.md](failure-matrix.md) §8）。

| 失うもの | 何が止まるか | 復旧の挙動（記録） | 失いうるデータ | 記録 |
|---|---|---|---|---|
| gateway プロセス（SIGKILL） | 全 API・invoke・publisher・dispatcher・scheduler・collector | 同じ `data_dir` で再起動すると、dispatch 済みの同期は `outcome_unknown` `Host.Restarted`、未 dispatch は `failed`、非同期は claim 期限後に再実行、未送信 outbox は publish、旧環境は terminate（process では recovery 0.2〜0.7 s、非同期 4〜5 s） | metrics・admission / scale 状態（メモリ）、invocation log のうち直近 1 flush 間隔（既定 200 ms）に届いた行（それ以前の行は `logs.db` に残る、ADR-0018）。client は応答を受け取らない（Idempotency-Key が無ければ結果を区別できない）。dispatch 済みで kill された attempt の利用量。**Firecracker では VMM が残り handler を実行し続け、reconcile の回収経路は未試験** | `sync_gateway_kill`、`async_kill_*`、`orphan_recovery_after_crash` |
| store の一時停止（`state.db` の書込み lock） | lease 内: 管理 API は 503 `Host.StoreUnavailable`、invoke は解放後に成功または 503 | lease 内なら解放後に再起動なしで回復（約 0.2 s） | なし（受付拒否は行を作らない） | `db_locked_within_lease` |
| store 停止が lease（既定 30 s + skew 2 s）を超える | heartbeat が書けず **gateway が自分を fence**、新規 invoke 503 | **再起動するまで 503**（他に回収する gateway がいなくても同じ）。再起動で全件収束 | なし（実測）。可用性は落ちる | `db_locked_past_lease`、[failure-matrix.md](failure-matrix.md) §6.2 |
| `state.db` の disk 喪失・破損 | すべて（関数定義・alias・受付済み非同期・outbox・DLQ・trigger・idempotency） | **backup が無いので復旧できない**（未試験・対象外） | 台帳の全体 | [failure-matrix.md](failure-matrix.md) §9 |
| nats-server の停止（SIGSTOP / SIGKILL） | 非同期の publish と配送。同期 invoke は影響なし | outbox 上限まで 202、以後 503 `queue_unavailable`。再開後に全件 terminal・副作用 1 回（recovery 3〜15 s） | なし（実測、store は無事） | `broker_sigstop`、`broker_sigkill` |
| JetStream store の disk 喪失 | 非同期の配送 | 台帳が残っていれば reaper が stall を検出して再 publish する設計（**未試験**） | 未 ACK の message（台帳から再構成できる前提） | [ADR-0013](adr/0013-async-dispatch-retry-dlq.md) §9 |
| object root の読み書き不能 | object 入力の受付（503 `object_store_unavailable`）と、その入力を読む非同期 run | inline 入力は 202 のまま。object を読めない run は数えない先送り、復旧後に成功、GC が孤児を回収 | disk 喪失なら全 object（鍵を失っても同じ） | `object_store_unavailable` |
| usage journal 満杯 / collector 停止 | 新規 invoke を 503 `Host.UsageJournalFull`（fail closed）、`/readyz` 503 | collector 再開・replay で ledger = journal、重複は無視 | journal の disk 喪失なら未回収の利用量 | `usage_journal_full_and_replay` |
| 予算 store 停止 / collector 遅延 | 全 tenant の新規 invoke を 503 `Host.BudgetStoreUnavailable` / `Host.BudgetUnknown` | 追いつけば受付再開 | disk 喪失なら予約・精算の記録 | [ADR-0016](adr/0016-budget-reservation-and-admission.md) §4 |
| control plane（管理 gateway）停止 | 配備・変更。data plane は配信済み設定の TTL と auth lease の間だけ invoke を継続 | TTL 切れで `Host.ConfigExpired`、再接続後 0.5 s で受付再開 | なし | `control_plane_outage`、[evidence/20260917T045347Z-split-process/](evidence/20260917T045347Z-split-process/) |
| worker（bridge / user process）の kill | その invocation | 同期 bridge kill は `outcome_unknown`、user process kill は `crash`、非同期は retry で成功 | 実行中の結果（外部副作用は不明のまま） | `worker_*` |
| **host そのもの**（電源断、OS 停止、disk 全損、laptop を閉じる） | **すべて**（ingress・gateway・台帳・queue・object・usage・予算・鍵） | 自動 failover なし。電源断・fsync の嘘・page cache の喪失は未試験。disk 全損なら復旧不能 | host 上の全データ（§4 の全行）。backup なし | [failure-matrix.md](failure-matrix.md) §9 |
| host 間の network partition | 該当なし（1 host） | — | — | 対象外 |

- 数値（recovery ms）は [evidence/chaos-20260917T115316Z/](evidence/chaos-20260917T115316Z/) の 4 回目で、設定値（claim 期限、lease、orphan grace）でほぼ決まる。**性能値や SLO ではない。**
- **PLT-4649 実行後に更新**: 最終受入の障害試験の結果（どの provider で、どのシナリオを通したか）。

---

## 6. 後続の設計判断

プロトタイプでは決めず、有償 β の前に owner が決めるもの。「推奨」は現時点の材料から言えることだけで、決定ではない。

### 6.1 HA・複数 host

| 論点 | 選択肢 | tradeoff | 現状の材料 |
|---|---|---|---|
| 台帳（store） | (a) TiDB adapter を製品化（RFC §15 どおり） / (b) SQLite + 複製（litestream 等） / (c) PostgreSQL 系 | (a) は既存 TiDB と揃うが、repository の async 化、outbox・trigger・dispatch の実装、TLS、hotspot 対策、`FOR UPDATE` の READ-COMMITTED の癖への対処が残る。(b) は単一 writer のままで HA にならない | TiDB は試験専用 adapter で 47/47 pass（1 PD / 1 TiKV / 1 TiDB、loopback）。未実装: `AsyncInvocationRepository`・`TriggerRepository`・`AsyncDispatchRepository`・budget store（[ADR-0003](adr/0003-execution-state-persistence.md)「残るもの（TiDB）」）。outbox claim と inline 出力 retention は TiDB で全件走査（[db-index-review.md](db-index-review.md) §2） |
| queue | (a) JetStream cluster（3 node 以上、`num_replicas = 3`） / (b) 台帳 queue を HA store に載せる | (a) は RAFT quorum・TLS・nkey / JWT の運用が増える。(b) は poll 負荷と store への集中 | 単一 node だけ検証。crash 後に `delivery_count` が戻る挙動は cluster でも前提にしない（台帳で数える） |
| object store | (a) S3 互換（国内保管契約のあるもの） / (b) 自前 MinIO 等 | 鍵管理・複製・region 間配置の責任分界が変わる | port は S3 互換を載せられる形（[ADR-0008](adr/0008-durable-queue-and-object-store.md)） |
| gateway / dispatcher | (a) 複数 gateway で同じ store を共有（lease・fencing） / (b) cell ごとに 1 dispatcher + 待機系 | admission・予算・outbox 上限・retry budget が **プロセスごと**なので、(a) では上限を store 側に移す必要がある | 2 gateway の fencing は同一 host で実測。admission の複数 gateway 共有は未実装（[threat-model.md](threat-model.md) §14-10） |
| fence 後の自動復帰 | (a) supervisor が `dispatcher.fenced` で再起動 / (b) 誰にも回収されていない lease の再取得を設計 | (b) は二重実行の危険と可用性の交換 | [failure-matrix.md](failure-matrix.md) §6.2 |
| worker host の喪失 | cell 内 N+1、VMM の host 間移送はしない | RFC §17.3 の基準 | 未設計 |

### 6.2 国内 DC・物理構成

| 選択肢 | tradeoff |
|---|---|
| (a) 国内データセンターの専用 bare metal（x86_64） | RFC §17.2 の推奨。電源・冷却・回線・物理アクセス・遠隔復旧・予備機を契約で得られる。調達・原価が確定しないと単価が決まらない |
| (b) 国内 cloud の bare metal / nested 可能な VM | 調達は速いが、nested virtualization の性能（本記録で cold p95 6 s 超）と所在地・operator アクセスの契約確認が要る |
| (c) 事務所 mini PC | 実験用 pool として有用だが、共通の電源・回線・建物で別障害ドメインにならない。**有償 β の origin にはしない**（RFC §17.2） |

判断に要る材料: x86_64 bare metal での cold / warm の再計測（§7 V1）、1 host あたりの環境密度と原価（§6.6）、契約上の所在地・operator アクセス条件。

### 6.3 鍵管理

| 論点 | 選択肢 | tradeoff |
|---|---|---|
| object・snapshot・trigger secret の鍵 | (a) 国内 region の KMS（envelope 暗号化、tenant ごとの data key） / (b) HSM / (c) host の file のまま運用手順で守る | (c) は現状。鍵の喪失 = データ喪失、root 権限者は全 tenant を読める。(a) は KMS の所在地・事業者依存を §4.1 の説明に含める必要 |
| `state.db`・usage・budget の保存時暗号化 | (a) disk 暗号化 / (b) 列暗号化 / (c) TiDB の TDE | (b) は index・検索と衝突 |
| rotation | object 鍵・snapshot 鍵は再暗号化の移行が必要、trigger secret は猶予期間の設計 | 現状は rotation 手順が無い（[threat-model.md](threat-model.md) §14-11・§14-17・§14-19） |
| token・secret binding | tachyon-apps `packages/secrets` / 利用者認証（Cognito）との接続 | 現状は gateway 設定ファイルの平文で、失効に再起動が要る |

### 6.4 実請求

| 論点 | 現状 | 決めること |
|---|---|---|
| 課金 pipeline | host 計測の UsageEvent v2 → local journal → ledger → 仮価格表（`provisional-dev-2026-09-v1`）。`billing_enabled` は設定で true にできない | ledger の複製・署名・外部 anchoring、既存 build metering（tachyon-apps）との統合 or 分離、請求書・決済・訂正・異議申立ての経路 |
| 価格 | 数値は仮置き。host 原価（idle・teardown・attempt の無い boot・cgroup CPU）は仮料金に含まない | 原価計測（§6.6）後の単価・無料枠。初期化時間を課金に含めるかの規約（RFC §16.2） |
| 計測の欠け | 不明区間は非課金、dispatch 済みで kill された attempt は利用量に出ない | この扱いを利用規約に明記するか |
| 法務・税務 | 未検討（本リポジトリの範囲外） | 特定商取引法の表示、消費税・インボイス、利用規約・SLA 条項、返金・補償。**技術文書では断定しない** |

### 6.5 サポート・abuse・SLA

| 論点 | 現状 | 決めること |
|---|---|---|
| abuse（採掘・spam・scan・巨大 log） | 止める手段は token の削除 + 再起動、function 削除、tunnel を落とす、予算 hard limit。tenant 単位の緊急停止 API・rate limit・本人確認なし | 検知指標、停止の権限者と時間目標、tenant 緊急停止 API、egress の既定（`none`）を維持するか、連絡窓口（`SECURITY.md` との対応） |
| サポート | 運用者・連絡先・on-call の定義なし。runbook は lab 向け | 受付経路、一次応答時間、障害時の告知、ログ閲覧の権限（operator アクセスの国内限定を含む） |
| SLA | 無い。可用性は未測定 | β は SLA なし（観測目標のみ）か、測定窓・除外条件・補償を定義した SLA を付けるか。§7 V5 の観測期間の結果を先に取る |

### 6.6 性能・原価

| 論点 | 現状の数値（aarch64 nested 1 host、参考値） | 判断に要るもの |
|---|---|---|
| cold 起動 | hello cold p50 / p95 5817 / 6037 ms、boot の throttled 比 0.99 | x86_64 bare metal での再計測、serial console 抑制・CPU quota 変更時の比較、drive 作成の cache |
| warm の基盤遅延 | p95 17〜31 ms | 同一 region・所定負荷（open loop）での再計測 |
| 環境 1 つの固定原価 | VMM RSS 約 38.7 MiB、cgroup 約 41 MiB（要求 256 MiB）、disk 約 268 MiB、休止中 CPU 0 | 1 host あたりの密度、memory overcommit の方針（休止は memory を返さない） |
| 同時実行 | pool 無効で 0.4〜0.5 件/s で頭打ち（4 vCPU の VM） | host の vCPU と起動並列度の関係、start-rate の設定値 |
| nested vs bare metal | nested の値しか無い | **bare metal の値が出るまで単価・SLA を決めない** |
| fsync のコスト | usage journal・予算 store・JetStream `sync_interval: always` の影響は未計測 | 同一 host での throughput 計測 |

### 6.7 Kata と Firecracker

| 選択肢 | tradeoff | 材料 |
|---|---|---|
| (a) Firecracker 直接（プロトタイプの形）を β の実行基盤にする | protocol・deadline・warm・clone の実測がある。Kubernetes・既存 Kata 運用と別の運用系になる。RFC §0 の方針と異なるので **RFC の改版が要る** | [ADR-0001](adr/0001-execution-provider-firecracker-first.md)、§1.2 |
| (b) RFC どおり Kata + CH に戻し、Kata adapter を書く | 既存 cluster と揃う。bridge の PID 1・vsock 共有・結果回収経路を先に解決する必要、稼働 sandbox の checkpoint は無い、CH は検証 host で未成立 | [ADR-0015](adr/0015-snapshot-restore-feasibility.md) |
| (c) 両方を trait の背後に置き、profile ごとに選ぶ | 試験行列と運用が倍になる | trait は用意済み |

決める前に: x86_64 bare metal で Firecracker の M1〜M13 を通す、CH を x86_64 / 専用 kernel で再評価する（ADR-0015 決定 5）、Knative 比較を RFC §6.4 の条件で行うか判断する。

### 6.8 KVM CI runner

- workflow（`.github/workflows/kvm-integration.yml`）と `kvm-gate` はあるが **runner 未登録**。登録には owner による runner group の制限、fork PR の承認、ephemeral runner、branch protection の required check 設定が要る（[ci.md](ci.md) §4.4・§5）。
- 選択肢: (a) 専用 bare metal を ephemeral runner に / (b) nested 可能な cloud VM / (c) 当面は手動の KVM 検証を PR に添付。public repository なので (c) を続ける場合も「KVM 未検証」を PR に明記する規則を維持する。

### 6.9 Tachyon Console 統合

- 置き場所・画面の移植方針は設計済み、**認証の橋渡しは未決**（[console-integration.md](console-integration.md) §3.1）。
- 選択肢: (a) tachyon-api の proxy が session を検証し短命 credential で gateway へ転送（推奨案、gateway に署名付き assertion の `IdentityProvider` 実装が要る） / (b) gateway が Cognito JWT を直接検証（CORS と gateway の公開が要る）。
- どちらでも Tachyon の operator id と serverless の `tn_…` tenant id の対応表、policy action と role（`deploy` / `invoke` / `redrive` / `operator`）の対応が要る。API 側の欠落（history の `dispatch`、dead-letter 一覧の `redrives`、cursor）を先に埋めるか。

### 6.10 jailer の uid・network namespace・IO 上限

| 論点 | 現状 | 選択肢 |
|---|---|---|
| VMM の uid | 全環境で uid 64000 を共有（chroot と PID namespace で分離） | 環境ごと / tenant ごとの uid 割当（uid pool の管理が要る） |
| network namespace | 使っていない。tap は host namespace に jail uid 所有で置く（ADR-0005 選択肢 C） | 環境ごとの netns（`--netns`）。nftables 規則と teardown の設計が変わる |
| IO 上限 | `io.max`・drive / NIC の `rate_limiter` なし。noisy neighbor の fsync 干渉は 1 回だけ小さかった | cgroup `io.max` と Firecracker `rate_limiter` の両方か片方。共有デバイスの性能に依存するので bare metal で測ってから値を決める |
| gateway の権限 | gateway が root で動く（jailer・cgroup・tap / nftables のため）。gateway の脆弱性 = host root | 特権操作を限定 API の node supervisor に分離（RFC §4） |

### 6.11 その他

- 安定した public ingress（ADR-0004 の R1〜R6: ドメイン、事業者 account、credential、常時稼働 host、運用者、token 発行・失効手順）と、TLS 終端の所在地（第三者 edge を通すか）。
- log の検索・転送・国内拠点（RFC §15 の OTel、payload を既定で記録しない）と、tenant ごとの log 保持期限。invocation log の永続化・保持期限・総量上限は ADR-0018 で入った。
- 保持期限の実装（invocation metadata、DLQ、redrive 記録、inline 入力、`dispatchers` 表）。
- OCI image の pull・署名検証、DB を使う Rust sample（RFC §22 M1）。
- 別の人間による runbook 追試（lab.sh の firecracker 経路は aarch64 nested の KVM で自動化 agent が実行済み）。

---

## 7. 国内有償 β へ進む条件

RFC §22 M4 の完了条件「データ所在を説明でき、故障復旧・課金訂正・abuse 対応を実施できる」を、確かめ方の付いた項目に分けた。**すべて未達**（2026-09-17）。

### 7.1 検証（技術的に示すもの）

| # | 条件 | 証明の方法 |
|---|---|---|
| V0 | プロトタイプ最終受入（PLT-4649）が通っている | PLT-4649 の証跡ディレクトリと acceptance の行。**PLT-4649 実行後に更新** |
| V1 | β の実機（国内 DC の x86_64 bare metal）で Firecracker の M1〜M13 と P1 E2E が通る | 同じ scripts（`scripts/kvm/smoke.sh`、`scripts/e2e/demo.sh`、`scripts/kvm/measure-isolation.sh`、`scripts/kvm/measure-warm.sh`）の evidence。M6・M11・M13 を含む |
| V2 | 非同期・usage・予算・故障マトリクスを **Firecracker provider で**通す | `scripts/queue/async-dispatch-e2e.sh`、`scripts/usage/usage-e2e.sh`、`scripts/usage/budget-e2e.sh`、`scripts/chaos/matrix.sh` を firecracker 設定で実行した evidence（[failure-matrix.md](failure-matrix.md) §8 の差分シナリオを含む） |
| V3 | 冗長構成で worker 1 台・store node 1 台・queue node 1 台の喪失に耐える | 複数 host の故障注入（電源断・disk 喪失・partition を含む）で `cv.accepted_never_lost`・`cv.usage_counted_once` が pass する evidence |
| V4 | backup から別の国内故障ドメインへ復旧できる | 実際の restore 試験で RPO / RTO を測った記録 |
| V5 | 性能と可用性を β 条件で観測する | bare metal での `scripts/kvm/bench.sh` 再計測（cold p95・warm p95）、open loop 負荷、一定期間の invoke 成功率の観測記録。仮目標（cold ≤ 3 s、warm ≤ 20 ms、99.9%）の判定を更新 |
| V6 | 隔離の残課題を閉じる | 環境ごとの uid または netns、IO 上限の実装と NOISY の再計測、gateway の特権分離の設計と試験 |
| V7 | 鍵・secret の運用 | KMS 由来の鍵での暗号化、rotation（再暗号化）の試験、secret backend 経由の配送、ログ・台帳・snapshot に secret が残らない検査の再実行 |
| V8 | KVM CI runner が required gate として動く | `kvm` job の成功した run の URL と、KVM 必要な PR が未実行で merge できないことの確認 |
| V9 | 別の人間が runbook で β 環境を再現・削除できる | 実施者名と command log を残した追試記録（[runbook.md](runbook.md) §9 の行） |
| V10 | データ所在の表（§4）が β 構成で全行埋まる | 各データの保存先の事業者・DC・国・複製先・backup 先・operator アクセスの一覧と、その根拠（契約書・構成の証跡）。label や location hint ではなく実際の配置 |

### 7.2 契約（外部と結ぶもの）

| # | 条件 | 証明の方法 |
|---|---|---|
| C1 | 国内データセンター（電源・冷却・回線・物理アクセス制御・遠隔対応）の契約 | 契約書と所在地・障害ドメインの記載。事務所 mini PC は対象外 |
| C2 | 国内保管が契約で保証された object storage / backup 先 / KMS | 事業者の契約・jurisdiction 条項（location hint ではない） |
| C3 | ドメイン・DNS・TLS 証明書・ingress 事業者の account（組織所有） | ADR-0004 R1〜R3 の充足記録、TLS 終端の所在地の明記 |
| C4 | 利用規約・プライバシーポリシー・SLA（付けるなら測定窓・除外・補償）・料金表 | 法務レビュー済みの文書 |
| C5 | 決済事業者・請求・税務（インボイス等）の契約と手順 | 契約と、テスト請求の記録 |
| C6 | 海外事業者に依存する範囲の開示（監視 SaaS、CDN、決済等） | RFC §17.1 の「国内完結」の分解に沿った一覧 |

### 7.3 運用（人と手順）

| # | 条件 | 証明の方法 |
|---|---|---|
| O1 | 運用者と連絡体制（on-call、エスカレーション、`SECURITY.md` との対応） | 担当表と、訓練で呼び出した記録 |
| O2 | 障害対応手順（host 喪失、store 喪失、queue 喪失、鍵喪失、fence 後の再起動） | β 構成の runbook と、各手順を実際に実施した訓練記録 |
| O3 | abuse 対応（検知、tenant 緊急停止、証拠保全、連絡） | 手順書と、緊急停止 API / 手順を実行した記録 |
| O4 | 課金訂正・異議申立て | 手順書と、ledger からの訂正を試行した記録 |
| O5 | 監査ログ（operator の操作、redrive、設定変更、鍵アクセス）の保全と保持 | 保持期間の定義と、監査ログを取り出した記録 |
| O6 | 保持期限・削除・export（invocation metadata、ログ、入出力、DLQ、snapshot） | 設定値と、期限到来で削除されたことの確認 |
| O7 | 監視と alert（`deploy/prometheus/alerts.yml` を β 構成で運用、課金未確定数・DLQ 量・queue oldest age） | 通知が担当者に届いた記録 |
| O8 | 変更管理（runtime profile の canary / rollback、kernel / VMM 更新時の snapshot 失効） | [ci.md](ci.md) §6 の手順を β 構成で実施した記録 |

---

## 8. 今回実行していないこと

PLT-4650 の作業（本文書と knowledge リポジトリへの draft PR）では、次のどれも行っていない。

- **公開**: public endpoint・ingress・DNS の作成、サービスの告知。
- **購入・契約**: データセンター、サーバ、cloud、ドメイン、KMS、決済事業者のいずれも。
- **本番移行**: 本番 cluster・既存 Tachyon（tachyon-apps）への配備や変更、Tachyon Console への統合。
- **実請求**: 請求・決済の有効化（`billing_enabled` は false のまま、仮価格表のまま）。
- Linear の issue の状態変更、knowledge PR の merge、PLT-4649 の実行。

作業は既存の文書・証跡・RFC・Linear の一覧の読み取りと、文書の追加だけである。

---

## 9. PLT-4649 実行後に更新する箇所

| 箇所 | 更新する内容 |
|---|---|
| §2.1「正常 invoke の可用性」 | 最終受入での失敗率（provider・負荷条件つき） |
| §2.1「durable async 受付の消失 0」 | 最終受入での受付件数・欠落件数 |
| §2.1「二重課金 0」 | 最終受入での usage の重複検査の結果 |
| §5 末尾 | 最終受入で通した障害試験（provider・シナリオ・結果） |
| §7.1 V0 | PLT-4649 の証跡と判定 |
| §1（全体） | 最終受入で Firecracker 経路を通した機能があれば、区分を process 実測から KVM 実測へ更新 |
