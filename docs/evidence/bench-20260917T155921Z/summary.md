# Benchmark bench-20260917T155921Z（PLT-4647）

**nested virtualization 上の aarch64 の記録であり、bare metal の x86_64 を代表しない。SLA・販売価格の根拠にしない。** 読み方と再実行は `docs/benchmark.md`。

## 条件

| 項目 | 値 |
|---|---|
| commit | `40a3c4ac148bfc7e8294a8ce7c00769667ecc6b0`（worktree dirty: false） |
| host | Linux lima-tsls-kvm 7.0.0-31-generic #31-Ubuntu SMP PREEMPT_DYNAMIC Sat Aug  1 03:33:01 UTC 2026 aarch64 GNU/Linux |
| CPU / memory | 4 vCPU（vendor Apple、model 0x000）/ MemTotal 8103308 KiB |
| physical host | Apple M4 (macOS, Darwin 25.6.0) -> Lima 'tsls-kvm' vmType vz with nested virtualization, 4 vCPU / 8 GiB guest; the Mac is a shared developer machine (see physical-host-load.tsv) |
| KVM / nested | rw / nested virtualization = true（virtualization: apple） |
| Firecracker / jailer | Firecracker v1.17.0 / Jailer v1.17.0 |
| guest kernel sha256 | `e3544b10603acbf3db492cb52e000d22ba202cb4b63b9add027565683e11c591`（firecracker-ci/v1.15/aarch64/vmlinux-6.1.155） |
| rootfs sha256 | `73b3e3b6cfccf93d5038c9fb20a0800207aad524e77e047bcceaf2d9ac399064`（67108864 bytes、bridge 5039960 bytes） |
| profile | firecracker microVM, jailer enabled, host cgroup v2 mode = required, egress none、256 MiB、500 m、timeout 30 s、revision max_concurrency product default (4) |
| gateway | release build、root via sudo (jailer + host cgroup required)、config `gateway-cold.toml` / `gateway-warm.toml`（`[pool] enabled = true`） |
| 負荷 | fresh host 5 回 / cold 20 回 / warm 20 回（間隔 250 ms）/ sweep 1,2,4,8 並列 × 各 24 request（cold / warm）/ idle 60 s |
| payload | hello=POST :invoke {"name":"bench"} http-axum=GET /http/ (router answers ok) cpu-burn=POST :invoke {"seconds":0.2} |
| client / seed | curl on the same host over loopback, one request per process, no keep-alive。none: payloads are fixed and requests are issued in a fixed order; nothing is randomised |
| host の混み具合 | VM 内の固定 CPU loop（ms、3 回）: start 79/79/77、before-hello 81/80/77、before-http-axum 80/80/80、before-cpu-burn 85/77/79、end 84/75/76。物理 host の 1 分 load average: p50 11.97 / max 17.19（n=173、`physical-host-load.tsv`） |

percentile は nearest-rank。client の分布は **失敗を含む全 attempt**、内訳の分布はその値を持つ attempt（n を併記）。外れ値は除外していない。生データは `attempts.jsonl`（1 request 1 行）と `invocations.jsonl`。

## 1. first response（ms、p50 / p95 / p99）

| sample | scenario | n | 失敗率 | start kind | client | queue | boot | init | resume | readiness | handler | host platform（total−handler） | client−total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| hello | fresh-miss | 5 | 0 | cold=5 | 5931 / 6267 / 6267 | 0 / 0 / 0 | 5217 / 5520 / 5520 | 493 / 504 / 504 | - / - / - | - / - / - | 104 / 111 / 111 | 5720 / 6020 / 6020 | 94 / 148 / 148 |
| hello | cold | 20 | 0 | cold=20 | 5819 / 6059 / 6086 | 0 / 0 / 0 | 5136 / 5318 / 5319 | 485 / 504 / 511 | - / - / - | - / - / - | 107 / 146 / 168 | 5624 / 5814 / 5827 | 92 / 151 / 186 |
| hello | warm-prime | 1 | 0 | cold=1 | 5755 / 5755 / 5755 | 0 / 0 / 0 | 5175 / 5175 / 5175 | 482 / 482 / 482 | - / - / - | - / - / - | 71 / 71 / 71 | 5705 / 5705 / 5705 | -21 / -21 / -21 |
| hello | warm | 20 | 0 | warm=20 | 51 / 61 / 62 | 0 / 0 / 0 | - / - / - | - / - / - | 1 / 1 / 1 | 3 / 12 / 14 | 38 / 40 / 40 | 6 / 15 / 17 | 7 / 10 / 14 |
| http-axum | fresh-miss | 5 | 0 | cold=5 | 5827 / 5959 / 5959 | 0 / 0 / 0 | 5125 / 5226 / 5226 | 493 / 511 / 511 | - / - / - | - / - / - | 114 / 153 / 153 | 5584 / 5746 / 5746 | 96 / 98 / 98 |
| http-axum | cold | 20 | 0 | cold=20 | 5699 / 5804 / 5821 | 0 / 0 / 0 | 4986 / 5060 / 5068 | 497 / 507 / 545 | - / - / - | - / - / - | 107 / 150 / 167 | 5484 / 5584 / 5607 | 92 / 154 / 207 |
| http-axum | warm-prime | 1 | 0 | cold=1 | 5799 / 5799 / 5799 | 0 / 0 / 0 | 5154 / 5154 / 5154 | 498 / 498 / 498 | - / - / - | - / - / - | 160 / 160 / 160 | 5660 / 5660 / 5660 | -21 / -21 / -21 |
| http-axum | warm | 20 | 0 | warm=20 | 48 / 52 / 81 | 0 / 0 / 0 | - / - / - | - / - / - | 1 / 1 / 1 | 3 / 9 / 12 | 36 / 39 / 59 | 5 / 10 / 15 | 6 / 7 / 8 |
| cpu-burn | fresh-miss | 5 | 0 | cold=5 | 5728 / 5895 / 5895 | 0 / 0 / 0 | 4916 / 5053 / 5053 | 418 / 490 / 490 | - / - / - | - / - / - | 305 / 310 / 310 | 5333 / 5493 / 5493 | 92 / 100 / 100 |
| cpu-burn | cold | 20 | 0 | cold=20 | 5638 / 5767 / 5810 | 0 / 0 / 0 | 4827 / 4956 / 4995 | 412 / 457 / 476 | - / - / - | - / - / - | 303 / 322 / 352 | 5251 / 5381 / 5413 | 90 / 98 / 100 |
| cpu-burn | warm-prime | 1 | 0 | cold=1 | 5743 / 5743 / 5743 | 0 / 0 / 0 | 4982 / 4982 / 4982 | 467 / 467 / 467 | - / - / - | - / - / - | 305 / 305 / 305 | 5458 / 5458 / 5458 | -20 / -20 / -20 |
| cpu-burn | warm | 20 | 0 | warm=20 | 288 / 305 / 312 | 0 / 0 / 0 | - / - / - | - / - / - | 1 / 1 / 1 | 3 / 3 / 8 | 277 / 292 / 299 | 6 / 6 / 10 | 7 / 9 / 14 |

- `fresh-miss`: gateway 停止 → data_dir と workdir を削除 → `drop_caches` → gateway 起動 → create / deploy → 最初の invoke（kernel・rootfs・VMM・artifact が page cache に無い）。n は fresh host の試行数で、20 未満のため p95 / p99 は最大値に近い。
- `cold`: 同じ gateway、pool 無効。page cache は温まっているが **function drive と scratch drive は環境ごとに毎回作る**（drive の cache は実装に無い）。
- `warm`: pool 有効の gateway で 1 回 prime した後の連続 invoke。warm にならなかった attempt も除外せず含める（start kind の列）。
- boot / init は cold attempt だけ、resume / readiness は warm attempt だけが持つ。
- 2 attempt になった invocation は無い。
- 失敗した attempt は無い。

warm のうち start kind が warm の attempt だけ（参考）:

| sample | n | client | host platform | client platform（client−handler） |
|---|---|---|---|---|
| hello | 20 | 51 / 61 / 62 | 6 / 15 / 17 | 13 / 22 / 23 |
| http-axum | 20 | 48 / 52 / 81 | 5 / 10 / 15 | 12 / 15 / 22 |
| cpu-burn | 20 | 288 / 305 / 312 | 6 / 6 / 10 | 13 / 15 / 20 |

gateway の起動（start → readyz 200）: fresh host 483 / 817 / 817（n=15）、page cache が温まった状態 474 / 478 / 478（n=3）。deploy（CLI、upload + revision ready）: 280 / 290 / 290（n=15）。

## 2. 同時実行（sweep）

| sample | pool | 並列 | n | 成功 | 失敗率 | 429 | 503 | 504 | start kind | 成功/秒 | client p50 / p95 / p99 | queue p50 / p95 / p99 | host platform p50 / p95 / p99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| hello | off | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.17 | 5757 / 5855 / 5868 | 0 / 0 / 0 | 5540 / 5649 / 5659 |
| hello | off | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.31 | 6235 / 6821 / 6843 | 0 / 0 / 0 | 5941 / 6606 / 6640 |
| hello | off | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.51 | 7750 / 8490 / 8595 | 0 / 0 / 0 | 7432 / 8176 / 8273 |
| hello | off | 8 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.51 | 14660 / 15998 / 16094 | 7223 / 8597 / 8798 | 14388 / 15782 / 15874 |
| hello | on | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 3.16 | 41 / 76 / 6157 | 0 / 0 / 0 | 4 / 5 / 6071 |
| hello | on | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 4 | 47 / 70 / 5306 | 0 / 0 / 0 | 4 / 13 / 5219 |
| hello | on | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=2 warm=22 | 3.6 | 59 / 6153 / 6262 | 0 / 0 / 0 | 4 / 6006 / 6182 |
| hello | on | 8 | 24 | 24 | 0 | 0 | 0 | 0 | warm=24 | 70.38 | 114 / 175 / 196 | 44 / 83 / 100 | 53 / 88 / 108 |
| http-axum | off | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.17 | 5771 / 5883 / 5963 | 0 / 0 / 0 | 5540 / 5656 / 5672 |
| http-axum | off | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.32 | 6047 / 6887 / 6966 | 0 / 0 / 0 | 5838 / 6679 / 6759 |
| http-axum | off | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.54 | 7110 / 8848 / 8868 | 0 / 0 / 0 | 6761 / 8473 / 8578 |
| http-axum | off | 8 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.54 | 14164 / 15618 / 15709 | 7032 / 8243 / 8301 | 13883 / 15348 / 15551 |
| http-axum | on | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 3.49 | 41 / 76 / 5530 | 0 / 0 / 0 | 3 / 8 / 5440 |
| http-axum | on | 2 | 24 | 24 | 0 | 0 | 0 | 0 | warm=24 | 28.3 | 43 / 105 / 108 | 0 / 0 / 0 | 3 / 12 / 12 |
| http-axum | on | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=2 warm=22 | 4 | 48 / 5615 / 5644 | 0 / 0 / 0 | 4 / 5467 / 5495 |
| http-axum | on | 8 | 24 | 24 | 0 | 0 | 0 | 0 | warm=24 | 55.56 | 105 / 153 / 158 | 43 / 77 / 96 | 47 / 90 / 102 |
| cpu-burn | off | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.17 | 5714 / 5866 / 5913 | 0 / 0 / 0 | 5302 / 5428 / 5509 |
| cpu-burn | off | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.32 | 6163 / 6719 / 6747 | 0 / 0 / 0 | 5717 / 6242 / 6331 |
| cpu-burn | off | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.55 | 6944 / 8057 / 8188 | 0 / 0 / 0 | 6537 / 7556 / 7661 |
| cpu-burn | off | 8 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.55 | 14081 / 14904 / 15089 | 7051 / 7718 / 7861 | 13608 / 14431 / 14647 |
| cpu-burn | on | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 1.87 | 278 / 300 / 6144 | 0 / 0 / 0 | 4 / 6 / 5867 |
| cpu-burn | on | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 2.85 | 285 / 296 / 5119 | 0 / 0 / 0 | 3 / 7 / 4825 |
| cpu-burn | on | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=2 warm=22 | 3.48 | 285 / 5336 / 5366 | 0 / 0 / 0 | 4 / 5046 / 5077 |
| cpu-burn | on | 8 | 24 | 24 | 0 | 0 | 0 | 0 | warm=24 | 13.61 | 537 / 584 / 629 | 266 / 301 / 330 | 270 / 307 / 333 |

上限: gateway `[capacity] max_concurrency = 8`、`max_queue = 32`、`queue_timeout_seconds = 10`、revision の max_concurrency は上表の条件。総 request 数は並列度によらず固定（VM を飽和させないため）。
sweep の失敗は無い。

## 3. 資源原価

### 3.1 休止中（pool）の環境 1 つあたり

| sample | 窓 s | env | 要求 memory MiB | VMM state | VMM RSS MiB（始 → 終） | cgroup memory.current MiB（始 → 終） | memory.peak MiB | anon / file MiB | VMM CPU tick 増分 | cgroup CPU usec 増分 | host disk（env dir + jail、重複除く）MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|
| hello | 60 | `env_01m2r1yqh17qm4acm07nn1bjqn` | 256 | S | 38.6 → 38.6 | 40.6 → 40.3 | 41 | 36.4 / 3.3 | 0 | 0 | 349.6 |
| http-axum | 60 | `env_01m2r2g9r91ecvwmdmxy33s4p3` | 256 | S | 38.8 → 38.8 | 41.3 → 40.8 | 41.3 | 36.6 / 3.3 | 0 | 0 | 349.6 |
| cpu-burn | 60 | `env_01m2r31fy223rw8k4b85rp6gz3` | 256 | S | 38.6 → 38.6 | 40.8 → 40.3 | 41 | 36.4 / 3.3 | 0 | 0 | 349.6 |

- hello: gateway RSS 33.1 → 33.1 MiB、gateway CPU tick 増分 18（CLK_TCK 100）、host MemAvailable 7295.9 → 7308.5 MiB
- http-axum: gateway RSS 35 → 35 MiB、gateway CPU tick 増分 18（CLK_TCK 100）、host MemAvailable 7287.9 → 7288 MiB
- cpu-burn: gateway RSS 34.7 → 34.7 MiB、gateway CPU tick 増分 16（CLK_TCK 100）、host MemAvailable 7289.3 → 7287.9 MiB

### 3.2 環境の寿命全体（provider が teardown 時に記録する cgroup の値。cold と warm の全環境）

| sample | 環境数 | memory.peak MiB p50 / p95 / max | CPU usage ms p50 / p95 / max | throttled ms p50 / p95 | throttled period 比 p50 / p95 | OOM kill |
|---|---|---|---|---|---|---|
| hello | 126 | 40.8 / 41.3 / 43 | 3126 / 4231 / 4933 | 3043 / 4007 | 0.99 / 1 | 0 |
| http-axum | 125 | 41 / 41.5 / 43.2 | 3037 / 4062 / 5369 | 2960 / 3851 | 0.99 / 1 | 0 |
| cpu-burn | 126 | 40.8 / 41.4 / 43.1 | 3101 / 3865 / 12102 | 3015 / 3709 | 0.99 / 1 | 0 |

### 3.3 gateway（host 側 bridge session）と pool の規模

| sample | 時点 | pool 内の環境 | gateway RSS MiB | host MemAvailable MiB |
|---|---|---|---|---|
| hello | gateway-empty | 0 | 31.2 | 7303 |
| hello | gateway-empty | 0 | 31.8 | 7305.5 |
| hello | gateway-empty | 0 | 32.2 | 7311.3 |
| hello | gateway-empty | 0 | 30.2 | 7308.2 |
| hello | gateway-empty | 0 | 30.2 | 7316.8 |
| hello | warm-gateway-empty | 0 | 30.1 | 7338.5 |
| hello | idle-start | 1 | 33.1 | 7295.9 |
| hello | idle-end | 1 | 33.1 | 7308.5 |
| hello | warm-after-sweep | 4 | 43.4 | 7167.6 |
| http-axum | gateway-empty | 0 | 31.4 | 7335 |
| http-axum | gateway-empty | 0 | 30.3 | 7351 |
| http-axum | gateway-empty | 0 | 31.1 | 7341.8 |
| http-axum | gateway-empty | 0 | 30.3 | 7350.6 |
| http-axum | gateway-empty | 0 | 31.6 | 7340 |
| http-axum | warm-gateway-empty | 0 | 32 | 7319.9 |
| http-axum | idle-start | 1 | 35 | 7287.9 |
| http-axum | idle-end | 1 | 35 | 7288 |
| http-axum | warm-after-sweep | 4 | 45.8 | 7161.1 |
| cpu-burn | gateway-empty | 0 | 30.2 | 7341.2 |
| cpu-burn | gateway-empty | 0 | 30.2 | 7321.4 |
| cpu-burn | gateway-empty | 0 | 31.6 | 7327.5 |
| cpu-burn | gateway-empty | 0 | 31.5 | 7322.8 |
| cpu-burn | gateway-empty | 0 | 31.3 | 7339.1 |
| cpu-burn | warm-gateway-empty | 0 | 31.7 | 7328.7 |
| cpu-burn | idle-start | 1 | 34.7 | 7289.3 |
| cpu-burn | idle-end | 1 | 34.7 | 7287.9 |
| cpu-burn | warm-after-sweep | 4 | 43.1 | 7167.8 |

- hello sweep 後に pool に残った環境 4（上限 max_idle_per_revision = 8）: cgroup memory.current p50 40.9 MiB / max 41 MiB、VMM RSS p50 38.5 MiB、disk p50 349.6 MiB
- http-axum sweep 後に pool に残った環境 4（上限 max_idle_per_revision = 8）: cgroup memory.current p50 41.1 MiB / max 41.5 MiB、VMM RSS p50 38.7 MiB、disk p50 349.6 MiB
- cpu-burn sweep 後に pool に残った環境 4（上限 max_idle_per_revision = 8）: cgroup memory.current p50 40.8 MiB / max 41.4 MiB、VMM RSS p50 38.6 MiB、disk p50 349.6 MiB

### 3.4 disk と転送量

| sample | scenario | function drive bytes p50 | scratch drive 作成 ms p50 / p95（cold） | request body bytes p50 | response body bytes p50 | response header bytes p50 |
|---|---|---|---|---|---|---|
| hello | cold | 10485760 | 2 / 3 | 16 | 84 | 259 |
| hello | warm | 10485760 | - / - | 16 | 84 | 259 |
| http-axum | cold | 10485760 | 3 / 4 | 0 | 2 | 267 |
| http-axum | warm | 10485760 | - / - | 0 | 2 | 267 |
| cpu-burn | cold | 10485760 | 2 / 4 | 15 | 70 | 259 |
| cpu-burn | warm | 10485760 | - / - | 15 | 70 | 259 |

guest の network 転送量: egress none（NIC 0、`env.network_interfaces`）のため 0。client ↔ gateway は loopback の HTTP で、上表の bytes がそのすべて。gateway ↔ guest の vsock の byte 数は計測していない（未測定）。

## 4. RFC 仮目標との比較

| 目標 | 判定 | 測定値 | 理由 |
|---|---|---|---|
| cold first response: small Rust function, image cache hit, p95 <= 3 s (RFC §19) | 未達 | `{"sample":"hello","n":20,"failure_rate":0,"client_p95_ms":6059,"client_p99_ms":6086,"environment_boot_p95_ms":5318,"runtime_init_p95_ms":504,"cgroup_throttled_period_ratio_p50":0.99}` | p95 above 3000 ms on this host; see environment_boot_ms and the cgroup throttling ratio for where the time goes |
| cold first response, image cache miss: reported separately (RFC §19) | 記録のみ（目標値なし） | `[{"sample":"hello","n":5,"client_p95_ms":6267,"client_max_ms":6267,"failure_rate":0},{"sample":"http-axum","n":5,"client_p95_ms":5959,"client_max_ms":5959,"failure_rate":0},{"sample":"cpu-burn","n":5,"client_p95_ms":5895,"client_max_ms":5895,"failure_rate":0}]` | RFC gives no number for the miss case; recorded so it is not mixed into the hit percentiles |
| warm platform added latency p95 <= 20 ms, user handler excluded (RFC §19) | 達成（host 計測、全 sample） | `[{"sample":"hello","n":20,"start_kinds":{"warm":20},"failure_rate":0,"host_platform_p95_ms":15,"client_platform_p95_ms":22},{"sample":"http-axum","n":20,"start_kinds":{"warm":20},"failure_rate":0,"host_platform_p95_ms":10,"client_platform_p95_ms":15},{"sample":"cpu-burn","n":20,"start_kinds":{"warm":20},"failure_rate":0,"host_platform_p95_ms":6,"client_platform_p95_ms":15}]` | judged per sample on host_platform p95; host_platform = total_ms - handler_ms (queue, resume, readiness, response, bookkeeping) over every attempt of the warm group including any that fell back to cold; client_platform adds loopback HTTP and curl. RFC asks for same-region under a set load; this is loopback on one nested host, sequential |
| Fast Restore: end-to-end p95/p99 and cost better than cold (RFC §19) | 未測定 | `null` | snapshot restore is not implemented (Capabilities.snapshot_clone = unsupported); X1 adds it to this benchmark |
| normal invoke availability 99.9% (RFC §19) | 未測定 | `[{"sample":"hello","scenario":"fresh-miss","n":5,"failure_rate":0},{"sample":"hello","scenario":"cold","n":20,"failure_rate":0},{"sample":"hello","scenario":"warm-prime","n":1,"failure_rate":0},{"sample":"hello","scenario":"warm","n":20,"failure_rate":0},{"sample":"http-axum","scenario":"fresh-miss","n":5,"failure_rate":0},{"sample":"http-axum","scenario":"cold","n":20,"failure_rate":0},{"sample":"http-axum","scenario":"warm-prime","n":1,"failure_rate":0},{"sample":"http-axum","scenario":"warm","n":20,"failure_rate":0},{"sample":"cpu-burn","scenario":"fresh-miss","n":5,"failure_rate":0},{"sample":"cpu-burn","scenario":"cold","n":20,"failure_rate":0},{"sample":"cpu-burn","scenario":"warm-prime","n":1,"failure_rate":0},{"sample":"cpu-burn","scenario":"warm","n":20,"failure_rate":0}]` | a few hundred requests on one host are not an availability measurement; failure rates are listed for the record only |

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
