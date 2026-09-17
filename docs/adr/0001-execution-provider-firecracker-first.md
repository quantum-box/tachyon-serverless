# ADR-0001: 実行 provider は Firecracker を第一候補とし、ExecutionProvider trait の背後に置く（PLT-4616）

## ステータス

Accepted（2026-09-15）。測定結果により「Cloud Hypervisor へ fallback」の条件が発動しうる（§5）。

## コンテキスト

- 目的は、public な独立リポジトリ単体で「関数登録 → publish → invoke → 環境起動 → guest bridge → handler → result / log 回収 → rollback → 環境破棄」を通すこと（`docs/architecture.md` §1）。
- tachyon-apps には Kata Containers（handler `kata-clh` = Cloud Hypervisor）を k3s の RuntimeClass で使う設定と、Kubernetes Job で JobRun を実行する runner-controller がある（`docs/inventory-tachyon-apps.md` §3.3, §3.5）。ただし本リポジトリは tachyon-apps にも既存 cluster にも依存しない。
- 本リポジトリの protocol は guest bridge との双方向 stream（vsock / unix socket、長さ付き JSON frame）を前提にしている（`docs/protocol.md`）。
- 本決定は実機での測定結果を根拠にしていない（決定時の能力表はすべて `unverified`、`docs/inventory-tachyon-apps.md` §5）。その後に取った記録と、その範囲は §「残る測定」。

決めるのは「P1 で最初に実装し検証する provider」と「他の候補の位置づけ」であり、「最終的にどれが最も速いか」ではない。後者は測定してから決める。

## 選択肢

1. **Firecracker**（KVM 上の microVM。REST API を unix socket で提供。vsock、virtio-block、virtio-net、snapshot / restore）
2. **Cloud Hypervisor**（KVM 上の VMM。REST API / `ch-remote`。virtio-fs、pmem、hotplug、snapshot / restore）
3. **Kata Containers on k3s**（Kubernetes RuntimeClass 経由で Pod を microVM 内で実行。hypervisor は `kata-clh` = Cloud Hypervisor、または `kata-fc` = Firecracker）

## 比較

「公称」は upstream 文書の主張であり、本リポジトリの測定値ではない。リンクは執筆時点の upstream パス。

| 観点 | Firecracker | Cloud Hypervisor | Kata Containers（k3s、`kata-clh`） |
|---|---|---|---|
| Kubernetes 統合 | 無し（`firecracker-containerd` は別プロジェクト。Kata の `kata-fc` 経由でも可） | 無し（Kata の `kata-clh` 経由） | RuntimeClass で native。Pod overhead を RuntimeClass に宣言 |
| 起動時間（公称） | upstream サイトは「as little as 125 ms」と掲げる（[firecracker-microvm.github.io](https://firecracker-microvm.github.io/)）。本リポジトリ未測定 | README に具体的な公称値の明記なし。本リポジトリ未測定 | upstream 文書に headline の数値なし。VM 起動 + kata-agent + shim の合計。本リポジトリ未測定 |
| 設定面 | 小。`machine-config` / `boot-source` / `drives` / `vsock` / `network-interfaces` / `actions` / `snapshot` / `vm`（[API spec](https://github.com/firecracker-microvm/firecracker/blob/main/src/firecracker/swagger/firecracker.yaml)） | 中。device 種類が多い（virtio-fs、pmem、vdpa、hotplug）（[API](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/docs/api.md)） | 大。`configuration.toml`（hypervisor section）+ containerd + RuntimeClass + Pod spec + annotation + kata-deploy |
| host 強制 timeout | VMM 機能では無い。provider が `Shutdown` frame → 猶予 → VMM プロセスへ `SIGKILL`（`docs/protocol.md` §C） | 同左 | Job の `activeDeadlineSeconds`（秒粒度、Job controller 経由）。Pod 削除 → shim が VM を止める |
| network | tap を host で作り virtio-net に渡す。Firecracker は tap を作らない（[network-setup](https://github.com/firecracker-microvm/firecracker/blob/main/docs/network-setup.md)）。P1 は tap を作らず egress none | tap / macvtap。同様に host 側の責務 | CNI。egress 制御は NetworkPolicy（ADR-0030 の enforcement gate が示すとおり、適用と実効は同期しない） |
| filesystem | virtio-block のみ。rootfs（ro）+ function drive（ro、`mkfs.ext4 -d`）。virtio-fs 無し | virtio-block + virtio-fs（virtiofsd） | image は snapshotter、共有は virtio-fs / 9p を kata-agent 経由 |
| pause / resume | `PATCH /vm {state: Paused/Resumed}` | `vm.pause` / `vm.resume` | Kubernetes から露出しない |
| snapshot / restore | `PUT /snapshot/create` / `PUT /snapshot/load`。vsock・network・エントロピー等の制約が文書化（[snapshot-support](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md)） | snapshot / restore あり（[snapshot_restore](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/docs/snapshot_restore.md)） | Kubernetes から露出しない |
| cleanup の責任 | provider（本リポジトリ）: VMM プロセス、API socket、vsock uds、function drive、workdir。jailer を使えば chroot / cgroup も（[jailer](https://github.com/firecracker-microvm/firecracker/blob/main/docs/jailer.md)） | provider（本リポジトリ） | containerd shim + kubelet + Job TTL（`ttlSecondsAfterFinished`）。本リポジトリからは Job 削除しか出せない |
| 既存 Tachyon cluster への依存 | 無し（KVM のある Linux host 1 台） | 無し | 有り（k3s、kata-deploy、RuntimeClass、namespace quota、NetworkPolicy、runner-controller の規約） |
| guest ↔ host channel | vsock（guest CID 3 → host uds `<path>_<port>`）（[vsock](https://github.com/firecracker-microvm/firecracker/blob/main/docs/vsock.md)） | vsock（[vsock](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/docs/vsock.md)） | kata-agent が vsock を占有する。本リポジトリの bridge が別 port を使えるかは未確認 |
| 隔離境界 | KVM + VMM プロセス（seccomp、jailer 任意） | KVM + VMM プロセス | KVM + VMM + containerd + Pod security（seccomp の guest 内実効性は tachyon-apps ADR-0025 で未確認） |
| 本リポジトリの protocol との適合 | そのまま（vsock + bridge を `/sbin/tachyon-init` に置く） | ほぼそのまま（vsock + rootfs） | 不適合。bridge を PID 1 にできず、kata-agent と vsock を共有し、結果は termination message / logs 経由になる |

参考: [Firecracker design](https://github.com/firecracker-microvm/firecracker/blob/main/docs/design.md)、[Firecracker getting-started](https://github.com/firecracker-microvm/firecracker/blob/main/docs/getting-started.md)、[Firecracker prod-host-setup](https://github.com/firecracker-microvm/firecracker/blob/main/docs/prod-host-setup.md)、[Cloud Hypervisor README](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/README.md)、[Kata architecture](https://github.com/kata-containers/kata-containers/blob/main/docs/design/architecture/README.md)、[Kata hypervisors](https://github.com/kata-containers/kata-containers/blob/main/docs/hypervisors.md)、[kata-deploy](https://github.com/kata-containers/kata-containers/tree/main/tools/packaging/kata-deploy)、[Kubernetes RuntimeClass](https://kubernetes.io/docs/concepts/containers/runtime-class/)、[Kubernetes Pod Overhead](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-overhead/)、[Kubernetes Job termination](https://kubernetes.io/docs/concepts/workloads/controllers/job/#job-termination-and-cleanup)、[firecracker-containerd](https://github.com/firecracker-microvm/firecracker-containerd)。

## 決定

1. **P1 の実行 provider は Firecracker**（`crates/providers/firecracker`）。`docs/protocol.md` §C の guest 規約（rootfs、function drive、kernel cmdline、API 順序、terminate 手順）をそのまま実装する。
2. **provider は `ExecutionProvider` trait（`crates/provider-port/src/execution.rs`）の背後に置く。** domain / application は Firecracker を import しない。Firecracker 固有の型は `crates/providers/firecracker` から出さない。
3. **Kata / Cloud Hypervisor は後続の adapter。** それぞれ `ExecutionProvider` を実装する crate として追加でき、application は `ProviderKind` の値以外で違いを知らない。Kata adapter を書く場合は、bridge transport（vsock の共有）と結果回収経路を先に解決する（§6）。
4. **Cloud Hypervisor は fallback。** 検証 host（PLT-4615）で Firecracker が §5 の条件で失敗した場合、Cloud Hypervisor で同じ guest 規約を試す。その場合も trait と protocol は変えない。
5. **P1 の `Capabilities` は保守的に返す。** `snapshot_create` / `snapshot_clone` / `egress_restricted` / `egress_public_web` は `Unsupported{reason}`。`idle_quiesce` / `idle_resume` は PLT-4633 で実装し（`PATCH /vm {state: Paused/Resumed}`、上表「pause / resume」）、`scripts/kvm/measure-warm.sh`（`docs/kvm.md` §3.7）で実機計測を取った上で `Supported` に上げた。証跡は `docs/evidence/warm-20260916T162532Z/`（6 invocation 中 5 が warm、同一環境を 6 epoch 再利用、`resume_ms` / `readiness_ms` の中央値 9、休止中の VMM は 3 秒で CPU tick 0・RSS 38 MiB 据え置き）。aarch64 の nested virtualization での記録であり、x86_64 と bare metal、および再開拒否・無応答 guest といった失敗経路はテストでのみ確認している。`Supported` は `docs/evidence/` に証跡がある項目にだけ付ける。2026-09-17 時点で Firecracker provider が `Supported` を返すのは `create_terminate` / `observe` / `enforce_deadline` / `egress_none` / `enforce_resource_limits` / `idle_quiesce` / `idle_resume` で、証跡は `docs/evidence/kvm-20260915T080221Z/`（fc-smoke）、`docs/evidence/20260915T171631Z-firecracker/`（E2E 28/28）、`docs/evidence/isolation-20260916T020934Z/`（M8）、`docs/evidence/isolation-20260917T011555Z/`（M8・M9 の再計測と ephemeral storage。PLT-4622 で `enforce_resource_limits` を昇格。上限は vCPU 数・`mem_size_mib`・scratch drive の大きさで、VMM への host 側 cgroup は無い）、`docs/evidence/warm-20260916T162532Z/`（warm 再利用）。いずれも aarch64 の nested virtualization での記録で、x86_64 と bare metal は未測定。`host_metering` は測定が済むまで `Unverified{note}`。
6. **fake / process provider の結果は本決定を左右しない。** fake provider（`crates/providers/fake`）は pipeline の単体テスト、process provider（ADR-0002）は開発機での縦断確認のためにある。どちらも microVM の能力・時間・隔離について何も証明しない。「fake で通った」ことを根拠に Firecracker の能力を `Supported` にしない。

### Firecracker を第一にする理由

- protocol との適合が最も高い。bridge を `init=/sbin/tachyon-init` として PID 1 に置き、vsock 1 本で host と結べる。
- 設定面が最小で、provider の状態機械（create → connect → terminate → cleanup）を小さく保てる。cleanup 対象（process、API socket、vsock uds、drive、workdir）を `TerminateReport.cleaned` に列挙しやすい。
- 既存 cluster に依存しない。KVM のある Linux host 1 台で再現でき、public リポジトリの CI / 検証環境で扱える。
- 後続で warm / snapshot を検討するときの API（pause / snapshot）が upstream にある。ただし P1 では使わない。

### Kata を第一にしない理由

- Kubernetes、CNI、RuntimeClass、kata-deploy、namespace quota への依存が、独立リポジトリの目的と矛盾する。
- kata-agent が vsock と PID 1 を占有し、本リポジトリの bridge 規約（PID 1、vsock、`Ready` gate、frame での結果回収）と噛み合わない。
- deadline が Job controller 経由の秒粒度で、host 直接の terminate と粒度が合わない。
- tachyon-apps の Kata 設定は存在するが、本リポジトリが必要とする能力の証拠にはならない（`docs/inventory-tachyon-apps.md` §3.5）。

### Cloud Hypervisor を第一にしない理由

- Firecracker と同等に protocol と適合するが、設定面が広く、P1 で使わない機能（virtio-fs、hotplug）を抱える。
- Firecracker で失敗する原因（host の CPU 機能、nested virtualization、kernel の互換）は Cloud Hypervisor でも起こりうるため、両方を同時に実装する価値が測定前には無い。

## 結果（consequences）

- `crates/providers/firecracker` は `firecracker` バイナリ、`vmlinux`、`rootfs.ext4` を外部から受け取る（`config/gateway.{dev,firecracker}.toml` の `[provider.firecracker]`）。取得と検証は PLT-4615 のスクリプト。
- P1 の Firecracker provider は tap を作らない。`EgressProfile::Restricted` / `PublicWeb` を要求する revision は validation で拒否する。
- jailer は P1 で使わない（`docs/threat-model.md` §14-2）。使う場合は provider の cleanup 対象に chroot / cgroup が増える。
- aarch64 は第二対象。kernel cmdline に `keep_bootcon` を追加する以外の差分は測定で洗い出す。
- Kata adapter を書く場合、`docs/protocol.md` §A の transport を「vsock または unix socket」から拡張する必要がある。protocol crate の frame 定義は変えない。

## 残る測定（PLT-4615 / PLT-4621 / PLT-4622 / PLT-4627 で実施）

baseline profile は `docs/inventory-tachyon-apps.md` §6。結果は、`scripts/kvm/smoke.sh` が `docs/evidence/kvm-<UTC>/` に、`scripts/e2e/demo.sh` が `docs/evidence/<UTC>-<provider>/` に置く。

2026-09-16 時点の記録は `docs/evidence/kvm-20260915T080221Z/`（smoke。hello と timeout demo を 1 回ずつ）と `docs/evidence/20260915T125610Z-firecracker/`（E2E 27/27、11 attempt）、レビュー指摘修正の統合後の `docs/evidence/20260915T171631Z-firecracker/`（E2E 28/28、33 attempt、`environment_boot_ms` 2777〜5706 ms）だけ。どちらも Apple M4 上の Lima VM（vz、**nested virtualization**）、Linux aarch64、Firecracker v1.17.0、Firecracker CI の guest kernel `vmlinux-6.1.155`（`CI_VERSION=v1.15`）で取った。nested virtualization のオーバーヘッドを含み、baseline profile（x86_64 第一、N ≥ 20）を満たさないため、時間の値は代表値ではない。下表の「一部実測済み」は、この条件で括弧内の範囲を確認した記録があるという意味。§「受入規則」の M1〜M8 は M6 の冪等性と M8 が未測定のため、まだ満たしていない。

| # | 測定 | 合格の目安（数値は仮置き。測定後に更新） | 状況（2026-09-16） |
|---|---|---|---|
| M1 | `preflight`: `/dev/kvm`、`firecracker --version`、kernel / rootfs の digest、nested virtualization の有無 | すべて `ok = true` で `PreflightReport` が返る | 一部実測済み（aarch64 nested。`docs/evidence/kvm-20260915T080221Z/hello.json` と `docs/evidence/20260915T125610Z-firecracker/provider.json` の `preflight` 9 項目が ok。nested virtualization の有無は `PreflightReport` の項目に無く `docs/kvm.md` §5 に記録） / 未測定（x86_64） |
| M2 | `environment_boot_ms`（create 開始 → bridge の `Hello` 受信）、N ≥ 20 | 中央値と p95 を記録。目安は無い（初回は事実の記録） | 一部実測済み（aarch64 nested で起動と `Hello` 受信が成立: smoke 2 回、E2E 11 attempt。値は `docs/kvm.md` §5.5 の参考値） / 未測定（N ≥ 20 の中央値・p95、x86_64） |
| M3 | `runtime_init_ms`（`Hello` → `Ready`）hello サンプル | 同上 | 一部実測済み（M2 と同じ記録で handshake → `Ready` が成立） / 未測定（N ≥ 20 の中央値・p95、x86_64） |
| M4 | `handler_ms` / `total_ms` hello・http-axum | 同上 | 未測定（N ≥ 20 の中央値・p95。単発の参考値だけ `docs/kvm.md` §5.5 にある） |
| M5 | timeout kill: cpu-burn を `timeout_seconds = 5` で実行 | `execution_deadline + grace(1 s)` から terminate 完了まで ≤ 2 s、`Failed{Timeout}`、orphan 0 | 一部実測済み（host 強制 timeout → terminate → 残留なし: `docs/evidence/kvm-20260915T080221Z/timeout.json`、E2E step 18 は `timeout_seconds = 2` で 504 `Host.Timeout`） / 未測定（`timeout_seconds = 5` での「deadline + grace から terminate 完了まで ≤ 2 s」の単独測定） |
| M6 | terminate の冪等性と cleanup: 2 回目の `terminate_environment` が `was_running = false`、`cleaned` の全パスが存在しない | 100% | 一部実測済み（1 回目の terminate の `cleaned` と、その後に process・env dir・socket が残らないこと: `hello.json`・`timeout.json` の `leftovers`） / 未測定（2 回目の `terminate_environment`） |
| M7 | orphan 検査: N 回の invoke 後に firecracker プロセス、uds、drive、workdir が残らない | 0 件 | 一部実測済み（E2E 1 回分の invoke 後と gateway 停止後の orphan-check が clean: `docs/evidence/20260915T125610Z-firecracker/` の step 19・27） / 未測定（x86_64、回数を決めた繰り返し） |
| M8 | egress none: guest から `connect()` が失敗する（NIC が無い） | 失敗すること | 実測済み（aarch64 nested。3 宛先すべて `NetworkUnreachable`、DNS 解決なし、loopback のみ。`docs/evidence/isolation-20260916T020934Z/`） |
| M9 | resource 上限: `machine-config` の vCPU / memory が guest から見える値と一致し、超過 alloc が OOM で終わる | 一致、環境が `Failed{Crash}` に分類される | 実測済み（vCPU 1 一致、MemTotal 232 MiB / 要求 256 MiB、超過 alloc は `crash` / `Runtime.Crash`。ephemeral storage は PLT-4622 で scratch drive により強制し、64 MiB の環境で 58 MiB 書いて `ENOSPC`、host の空きは drive の確保分しか減らない。`docs/evidence/isolation-20260916T020934Z/`、`docs/evidence/isolation-20260917T011555Z/`） |
| M10 | 同時実行: `max_concurrency = 4` で 4 並列、5 本目が queue に入る | 429 / 504 の分類が仕様どおり | 未測定 |
| M11 | `OutcomeUnknown`: Running 中に firecracker プロセスを外部から kill | `OutcomeUnknown`、再実行なし、環境 terminate 済み | 未測定 |
| M12 | aarch64 で M1〜M5 | 同上 | 一部実測済み（M1〜M3・M5 の上記の範囲。nested virtualization 上だけで、bare metal は未測定） |
| M13 | 1 MiB payload / 6 MiB response の vsock 転送時間 | 記録 | 未測定 |

## 受入規則

- 本 ADR の「Firecracker 第一」は M1〜M8 が検証 host で通ることで確定する。M1〜M3 が通らなければ §「決定」4 の fallback を発動し、Cloud Hypervisor で M1〜M8 を実施する。両方通らなければ、決定を保留し PLT-4616 を再開する。
- fake provider / process provider のテスト結果は、上のいずれの判定にも使わない。
- `Capabilities` の `Supported` は対応する M# の evidence を引用できる場合にだけ付ける。

## fallback 条件（Cloud Hypervisor）

次のいずれかで発動する。

1. 検証 host で Firecracker が起動しない（KVM は使えるが `InstanceStart` が失敗する、CPU 機能の不足、kernel の互換問題）。
2. vsock で bridge が接続できない。
3. M5（timeout kill）で terminate が 2 秒以内に完了しない構造的理由がある。

発動後も trait・protocol・guest 規約は変えない。変えざるを得ない場合は本 ADR を改版する。
