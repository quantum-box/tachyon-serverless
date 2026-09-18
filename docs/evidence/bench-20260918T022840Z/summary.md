# Benchmark bench-20260918T022840Z（PLT-4647）

**nested virtualization 上の aarch64 の記録であり、bare metal の x86_64 を代表しない。SLA・販売価格の根拠にしない。** 読み方と再実行は `docs/benchmark.md`。

## 条件

| 項目 | 値 |
|---|---|
| commit | `4afd8c1d494ad055763151874fc369991c33da64`（worktree dirty: false） |
| host | Linux lima-tsls-kvm 7.0.0-31-generic #31-Ubuntu SMP PREEMPT_DYNAMIC Sat Aug  1 03:33:01 UTC 2026 aarch64 GNU/Linux |
| CPU / memory | 4 vCPU（vendor Apple、model 0x000）/ MemTotal 8103308 KiB |
| physical host | Apple M4 (macOS, Darwin 25.6.0) -> Lima 'tsls-kvm' vmType vz with nested virtualization, 4 vCPU / 8 GiB guest; the Mac is a shared developer machine (see physical-host-load.tsv) |
| KVM / nested | rw / nested virtualization = true（virtualization: apple） |
| Firecracker / jailer | Firecracker v1.17.0 / Jailer v1.17.0 |
| guest kernel sha256 | `947a09e2f89b488cdb5efb42eceb37a6ac414fcb0ddb1e5bba2ba0cd1a873a2d`（firecracker-ci/20260916-dcfc69b625d0-0/aarch64/vmlinux-6.18.48） |
| rootfs sha256 | `ea7b615273214cecd5b45990f999ea585103a557cb3477cc37dfc8073d7d0fae`（67108864 bytes、bridge 5039960 bytes） |
| profile | firecracker microVM, jailer enabled, host cgroup v2 mode = required, egress none、256 MiB、500 m、timeout 30 s、revision max_concurrency product default (4) |
| gateway | release build、root via sudo (jailer + host cgroup required)、config `gateway-cold.toml` / `gateway-warm.toml`（`[pool] enabled = true`） |
| 負荷 | fresh host 5 回 / cold 20 回 / warm 20 回（間隔 250 ms）/ sweep 1,2,4,8 並列 × 各 24 request（cold / warm）/ idle 60 s |
| payload | hello=POST :invoke {"name":"bench"} http-axum=GET /http/ (router answers ok) cpu-burn=POST :invoke {"seconds":0.2} |
| client / seed | curl on the same host over loopback, one request per process, no keep-alive。none: payloads are fixed and requests are issued in a fixed order; nothing is randomised |
| host の混み具合 | VM 内の固定 CPU loop（ms、3 回）: start 232/251/254。物理 host の 1 分 load average: p50 69.12 / max 69.12（n=1、`physical-host-load.tsv`） |

percentile は nearest-rank。client の分布は **失敗を含む全 attempt**、内訳の分布はその値を持つ attempt（n を併記）。外れ値は除外していない。生データは `attempts.jsonl`（1 request 1 行）と `invocations.jsonl`。

## 1. first response（ms、p50 / p95 / p99）

| sample | scenario | n | 失敗率 | start kind | client | queue | boot | init | resume | readiness | handler | host platform（total−handler） | client−total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|

- `fresh-miss`: gateway 停止 → data_dir と workdir を削除 → `drop_caches` → gateway 起動 → create / deploy → 最初の invoke（kernel・rootfs・VMM・artifact が page cache に無い）。n は fresh host の試行数で、20 未満のため p95 / p99 は最大値に近い。
- `cold`: 同じ gateway、pool 無効。page cache は温まっているが **function drive と scratch drive は環境ごとに毎回作る**（drive の cache は実装に無い）。
- `warm`: pool 有効の gateway で 1 回 prime した後の連続 invoke。warm にならなかった attempt も除外せず含める（start kind の列）。
- boot / init は cold attempt だけ、resume / readiness は warm attempt だけが持つ。
- 2 attempt になった invocation は無い。
- 失敗した attempt は無い。

warm のうち start kind が warm の attempt だけ（参考）:

| sample | n | client | host platform | client platform（client−handler） |
|---|---|---|---|---|

gateway の起動（start → readyz 200）: fresh host - / - / -（n=0）、page cache が温まった状態 - / - / -（n=0）。deploy（CLI、upload + revision ready）: - / - / -（n=0）。

## 2. 同時実行（sweep）

| sample | pool | 並列 | n | 成功 | 失敗率 | 429 | 503 | 504 | start kind | 成功/秒 | client p50 / p95 / p99 | queue p50 / p95 / p99 | host platform p50 / p95 / p99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|

上限: gateway `[capacity] max_concurrency = 8`、`max_queue = 32`、`queue_timeout_seconds = 10`、revision の max_concurrency は上表の条件。総 request 数は並列度によらず固定（VM を飽和させないため）。
sweep の失敗は無い。

## 3. 資源原価

### 3.1 休止中（pool）の環境 1 つあたり

| sample | 窓 s | env | 要求 memory MiB | VMM state | VMM RSS MiB（始 → 終） | cgroup memory.current MiB（始 → 終） | memory.peak MiB | anon / file MiB | VMM CPU tick 増分 | cgroup CPU usec 増分 | host disk（env dir + jail、重複除く）MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|


### 3.2 環境の寿命全体（provider が teardown 時に記録する cgroup の値。cold と warm の全環境）

| sample | 環境数 | memory.peak MiB p50 / p95 / max | CPU usage ms p50 / p95 / max | throttled ms p50 / p95 | throttled period 比 p50 / p95 | OOM kill |
|---|---|---|---|---|---|---|
| hello | 0 | - / - / - | - / - / - | - / - | - / - | 0 |
| http-axum | 0 | - / - / - | - / - / - | - / - | - / - | 0 |
| cpu-burn | 0 | - / - / - | - / - / - | - / - | - / - | 0 |

### 3.3 gateway（host 側 bridge session）と pool の規模

| sample | 時点 | pool 内の環境 | gateway RSS MiB | host MemAvailable MiB |
|---|---|---|---|---|


### 3.4 disk と転送量

| sample | scenario | function drive bytes p50 | scratch drive 作成 ms p50 / p95（cold） | request body bytes p50 | response body bytes p50 | response header bytes p50 |
|---|---|---|---|---|---|---|

guest の network 転送量: egress none（NIC 0、`env.network_interfaces`）のため 0。client ↔ gateway は loopback の HTTP で、上表の bytes がそのすべて。gateway ↔ guest の vsock の byte 数は計測していない（未測定）。

## 4. RFC 仮目標との比較

| 目標 | 判定 | 測定値 | 理由 |
|---|---|---|---|
| cold first response: small Rust function, image cache hit, p95 <= 3 s (RFC §19) | 未測定 | `null` | hello cold was not run |
| cold first response, image cache miss: reported separately (RFC §19) | 未測定 | `[]` | RFC gives no number for the miss case; recorded so it is not mixed into the hit percentiles |
| warm platform added latency p95 <= 20 ms, user handler excluded (RFC §19) | 未測定 | `[]` | judged per sample on host_platform p95; host_platform = total_ms - handler_ms (queue, resume, readiness, response, bookkeeping) over every attempt of the warm group including any that fell back to cold; client_platform adds loopback HTTP and curl. RFC asks for same-region under a set load; this is loopback on one nested host, sequential |
| Fast Restore: end-to-end p95/p99 and cost better than cold (RFC §19) | 未測定 | `null` | snapshot restore is not implemented (Capabilities.snapshot_clone = unsupported); X1 adds it to this benchmark |
| normal invoke availability 99.9% (RFC §19) | 未測定 | `[]` | a few hundred requests on one host are not an availability measurement; failure rates are listed for the record only |

判定はこの host（aarch64、Apple M4 上の Lima vz、nested virtualization、4 vCPU）での値であり、RFC の想定（x86_64 の国内 host、同一 region の負荷）とは条件が違う。達成でも production の性能を示さず、未達でも bare metal で未達とは限らない。

## 5. P0 の Kata / Knative 条件との比較

P0（PLT-4613 / PLT-4616、`docs/inventory-tachyon-apps.md` §3.5・§5、ADR-0001）では **Kata / Cloud Hypervisor / Knative のどれも実行・測定していない**。したがって速さの数値比較はできず、ここに Knative / Kata の数値は書かない。比較できるのは運用構成と容量の前提だけである。

| 観点 | 本測定（Firecracker provider） | P0 の Kata（tachyon-apps の設定、未実行） | Knative（RFC §6.4、未構築） |
|---|---|---|---|
| 構成要素 | KVM host 1 台、gateway 1 プロセス（root、jailer + cgroup v2）、SQLite 台帳 | k3s + containerd + kata-deploy（`kata-clh`）+ RuntimeClass + namespace quota | k3s + Knative Serving（activator / autoscaler / queue-proxy）+ Kata RuntimeClass |
| 環境 1 つの固定 memory | 上の §3.1 / §3.2 の実測（要求 memory + VMM。cgroup memory.max = 要求 + 64 MiB） | RuntimeClass `overhead.podFixed` = cpu 250m / memory 256Mi（宣言値。実測なし） | Kata の overhead + queue-proxy sidecar（未測定） |
| scale to zero / warm | pool（`[pool]`）で休止、TTL で回収。休止中は CPU tick 0 を §3.1 で測定、memory は返さない | Pod を保持するだけでは実行外 CPU は止まらない（RFC §12.4） | activator 経由の scale-to-zero（未測定） |
| 同時実行の上限 | gateway `max_concurrency` / `max_queue` と revision `max_concurrency`、429 / 504 で拒否（§2） | ResourceQuota pods 20 | containerConcurrency と autoscaler（未測定） |
| 起動の失敗・後始末 | provider が VMM・jail・cgroup・tap を回収し、`cleanup.txt` で残留 0 を確認 | Job TTL と shim | Knative controller（未測定） |

RFC §6.4 の「同じ Kata・同じイメージ・同じ負荷で Knative と比較する」は未実施（Kata adapter も Knative 構成も本リポジトリに無い）。

## 6. 後始末

`cleanup.txt` を参照（gateway・firecracker・jailer プロセス、環境 cgroup、jail、tap、nft table、環境ディレクトリが 0 件であること）。
