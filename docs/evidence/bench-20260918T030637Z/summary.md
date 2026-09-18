# Benchmark bench-20260918T030637Z（PLT-4647）

**nested virtualization 上の aarch64 の記録であり、bare metal の x86_64 を代表しない。SLA・販売価格の根拠にしない。** 読み方と再実行は `docs/benchmark.md`。

## 条件

| 項目 | 値 |
|---|---|
| commit | `bbd650e13f2a421d7cfde1807ef322cfd166e5d9`（worktree dirty: false） |
| host | Linux lima-tsls-kvm 7.0.0-31-generic #31-Ubuntu SMP PREEMPT_DYNAMIC Sat Aug  1 03:33:01 UTC 2026 aarch64 GNU/Linux |
| CPU / memory | 4 vCPU（vendor Apple、model 0x000）/ MemTotal 8103308 KiB |
| physical host | Apple M4 (macOS, Darwin 25.6.0) -> Lima 'tsls-kvm' vmType vz with nested virtualization, 4 vCPU / 8 GiB guest; the Mac is a shared developer machine (see physical-host-load.tsv) |
| KVM / nested | rw / nested virtualization = true（virtualization: apple） |
| Firecracker / jailer | Firecracker v1.17.0 / Jailer v1.17.0 |
| guest kernel sha256 | `947a09e2f89b488cdb5efb42eceb37a6ac414fcb0ddb1e5bba2ba0cd1a873a2d`（firecracker-ci/20260916-dcfc69b625d0-0/aarch64/vmlinux-6.18.48） |
| rootfs sha256 | `13c63025baca9d63ec9416674c803f59e216cb8c385ec9156f132a9d6b27e0aa`（67108864 bytes、bridge 5039960 bytes） |
| profile | firecracker microVM, jailer enabled, host cgroup v2 mode = required, egress none、256 MiB、500 m、timeout 30 s、revision max_concurrency product default (4) |
| gateway | release build、root via sudo (jailer + host cgroup required)、config `gateway-cold.toml` / `gateway-warm.toml`（`[pool] enabled = true`） |
| 負荷 | fresh host 5 回 / cold 20 回 / warm 20 回（間隔 250 ms）/ sweep 1,2,4,8 並列 × 各 24 request（cold / warm）/ idle 60 s |
| payload | hello=POST :invoke {"name":"bench"} http-axum=GET /http/ (router answers ok) cpu-burn=POST :invoke {"seconds":0.2} |
| client / seed | curl on the same host over loopback, one request per process, no keep-alive。none: payloads are fixed and requests are issued in a fixed order; nothing is randomised |
| host の混み具合 | VM 内の固定 CPU loop（ms、3 回）: start 94/91/94、before-hello 98/93/90、before-http-axum 133/100/97、before-cpu-burn 84/82/81、end 130/146/137。物理 host の 1 分 load average: p50 8.23 / max 58.78（n=511、`physical-host-load.tsv`） |

percentile は nearest-rank。client の分布は **失敗を含む全 attempt**、内訳の分布はその値を持つ attempt（n を併記）。外れ値は除外していない。生データは `attempts.jsonl`（1 request 1 行）と `invocations.jsonl`。

## 1. first response（ms、p50 / p95 / p99）

| sample | scenario | n | 失敗率 | start kind | client | queue | boot | init | resume | readiness | handler | host platform（total−handler） | client−total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| hello | fresh-miss | 5 | 0 | cold=5 | 6412 / 7728 / 7728 | 0 / 0 / 0 | 5695 / 7038 / 7038 | 435 / 478 / 478 | - / - / - | - / - / - | 159 / 172 / 172 | 6138 / 7475 / 7475 | 105 / 107 / 107 |
| hello | cold | 20 | 0 | cold=20 | 8691 / 16284 / 16579 | 0 / 0 / 0 | 7638 / 14311 / 14511 | 678 / 1284 / 1314 | - / - / - | - / - / - | 178 / 386 / 415 | 8357 / 15546 / 15813 | 199 / 359 / 413 |
| hello | warm-prime | 1 | 0 | cold=1 | 8031 / 8031 / 8031 | 0 / 0 / 0 | 7179 / 7179 / 7179 | 579 / 579 / 579 | - / - / - | - / - / - | 89 / 89 / 89 | 7852 / 7852 / 7852 | 90 / 90 / 90 |
| hello | warm | 20 | 0 | warm=20 | 106 / 836 / 1027 | 0 / 0 / 0 | - / - / - | - / - / - | 1 / 1 / 1 | 2 / 26 / 34 | 37 / 47 / 57 | 16 / 49 / 80 | 24 / 797 / 1008 |
| http-axum | fresh-miss | 5 | 0 | cold=5 | 9131 / 11772 / 11772 | 0 / 0 / 0 | 7596 / 10056 / 10056 | 617 / 1096 / 1096 | - / - / - | - / - / - | 131 / 284 / 284 | 8533 / 11169 / 11169 | 319 / 655 / 655 |
| http-axum | cold | 20 | 0 | cold=20 | 6928 / 11874 / 12313 | 0 / 0 / 0 | 6014 / 9776 / 10368 | 520 / 1100 / 1355 | - / - / - | - / - / - | 171 / 270 / 287 | 6600 / 11387 / 11946 | 159 / 260 / 437 |
| http-axum | warm-prime | 1 | 0 | cold=1 | 10489 / 10489 / 10489 | 0 / 0 / 0 | 9375 / 9375 / 9375 | 873 / 873 / 873 | - / - / - | - / - / - | 184 / 184 / 184 | 10310 / 10310 / 10310 | -5 / -5 / -5 |
| http-axum | warm | 20 | 0 | warm=20 | 65 / 141 / 146 | 0 / 0 / 0 | - / - / - | - / - / - | 1 / 1 / 4 | 3 / 4 / 4 | 47 / 89 / 109 | 7 / 10 / 30 | 10 / 25 / 32 |
| cpu-burn | fresh-miss | 5 | 0 | cold=5 | 8138 / 8887 / 8887 | 0 / 0 / 0 | 6930 / 7743 / 7743 | 515 / 606 / 606 | - / - / - | - / - / - | 323 / 393 / 393 | 7535 / 8364 / 8364 | 204 / 223 / 223 |
| cpu-burn | cold | 20 | 0 | cold=20 | 8282 / 13624 / 14270 | 0 / 0 / 0 | 6597 / 11892 / 12403 | 699 / 1022 / 1104 | - / - / - | - / - / - | 388 / 486 / 502 | 7756 / 12934 / 13426 | 213 / 342 / 515 |
| cpu-burn | warm-prime | 1 | 0 | cold=1 | 8449 / 8449 / 8449 | 0 / 0 / 0 | 7040 / 7040 / 7040 | 802 / 802 / 802 | - / - / - | - / - / - | 470 / 470 / 470 | 7856 / 7856 / 7856 | 123 / 123 / 123 |
| cpu-burn | warm | 20 | 0 | warm=20 | 320 / 459 / 796 | 0 / 0 / 0 | - / - / - | - / - / - | 1 / 1 / 1 | 2 / 3 / 5 | 272 / 294 / 297 | 9 / 48 / 80 | 38 / 131 / 540 |

- `fresh-miss`: gateway 停止 → data_dir と workdir を削除 → `drop_caches` → gateway 起動 → create / deploy → 最初の invoke（kernel・rootfs・VMM・artifact が page cache に無い）。n は fresh host の試行数で、20 未満のため p95 / p99 は最大値に近い。
- `cold`: 同じ gateway、pool 無効。page cache は温まっているが **function drive と scratch drive は環境ごとに毎回作る**（drive の cache は実装に無い）。
- `warm`: pool 有効の gateway で 1 回 prime した後の連続 invoke。warm にならなかった attempt も除外せず含める（start kind の列）。
- boot / init は cold attempt だけ、resume / readiness は warm attempt だけが持つ。
- 2 attempt になった invocation は無い。
- 失敗した attempt は無い。

warm のうち start kind が warm の attempt だけ（参考）:

| sample | n | client | host platform | client platform（client−handler） |
|---|---|---|---|---|
| hello | 20 | 106 / 836 / 1027 | 16 / 49 / 80 | 60 / 834 / 1024 |
| http-axum | 20 | 65 / 141 / 146 | 7 / 10 / 30 | 17 / 32 / 62 |
| cpu-burn | 20 | 320 / 459 / 796 | 9 / 48 / 80 | 47 / 179 / 547 |

gateway の起動（start → readyz 200）: fresh host 663 / 3296 / 3296（n=15）、page cache が温まった状態 596 / 599 / 599（n=3）。deploy（CLI、upload + revision ready）: 298 / 376 / 376（n=15）。

## 2. 同時実行（sweep）

| sample | pool | 並列 | n | 成功 | 失敗率 | 429 | 503 | 504 | start kind | 成功/秒 | client p50 / p95 / p99 | queue p50 / p95 / p99 | host platform p50 / p95 / p99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| hello | off | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.11 | 9023 / 14508 / 15259 | 0 / 0 / 0 | 8416 / 13875 / 14803 |
| hello | off | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.16 | 11812 / 18082 / 19137 | 0 / 0 / 0 | 11410 / 17545 / 17667 |
| hello | off | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.25 | 16129 / 17478 / 17921 | 0 / 0 / 0 | 14905 / 16818 / 17347 |
| hello | off | 8 | 24 | 12 | 0.5 | 0 | 0 | 12 | cold=12 none=12 | 0.28 | 10612 / 17985 / 18548 | 2724 / 2998 / 2998 | 14796 / 18194 / 18194 |
| hello | on | 1 | 24 | 24 | 0 | 0 | 0 | 0 | warm=24 | 7.5 | 69 / 242 / 316 | 0 / 0 / 0 | 12 / 56 / 65 |
| hello | on | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 2.77 | 87 / 881 / 6796 | 0 / 0 / 12 | 11 / 128 / 6570 |
| hello | on | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=2 warm=22 | 1.61 | 167 / 13497 / 13829 | 0 / 0 / 0 | 37 / 13321 / 13503 |
| hello | on | 8 | 24 | 24 | 0 | 0 | 0 | 0 | warm=24 | 14.15 | 360 / 1196 / 1221 | 80 / 601 / 609 | 131 / 882 / 955 |
| http-axum | off | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.13 | 6384 / 10493 / 16418 | 0 / 0 / 0 | 6133 / 10090 / 14212 |
| http-axum | off | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.24 | 6723 / 14501 / 16271 | 0 / 0 / 0 | 6481 / 13937 / 15705 |
| http-axum | off | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.34 | 10760 / 15905 / 15928 | 0 / 0 / 0 | 10228 / 15181 / 15275 |
| http-axum | off | 8 | 24 | 17 | 0.292 | 0 | 0 | 7 | cold=17 none=7 | 0.27 | 15841 / 31497 / 31995 | 7678 / 9406 / 9406 | 17675 / 31565 / 31565 |
| http-axum | on | 1 | 24 | 24 | 0 | 0 | 0 | 0 | warm=24 | 15.83 | 42 / 69 / 112 | 0 / 0 / 0 | 4 / 23 / 25 |
| http-axum | on | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 3.56 | 42 / 96 / 6024 | 0 / 0 / 0 | 4 / 31 / 5861 |
| http-axum | on | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=2 warm=22 | 3.4 | 49 / 6722 / 6800 | 0 / 0 / 0 | 4 / 6617 / 6686 |
| http-axum | on | 8 | 24 | 24 | 0 | 0 | 0 | 0 | warm=24 | 43.8 | 134 / 169 / 171 | 48 / 71 / 79 | 70 / 90 / 104 |
| cpu-burn | off | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.11 | 8992 / 11189 / 12361 | 0 / 0 / 0 | 8304 / 10572 / 11671 |
| cpu-burn | off | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.16 | 12910 / 14367 / 14826 | 0 / 0 / 0 | 11952 / 13773 / 13971 |
| cpu-burn | off | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.31 | 12490 / 13786 / 14159 | 0 / 0 / 0 | 11653 / 12810 / 13193 |
| cpu-burn | off | 8 | 24 | 13 | 0.458 | 0 | 0 | 11 | cold=13 none=11 | 0.27 | 11386 / 18056 / 24714 | 2966 / 9657 / 9657 | 13741 / 23556 / 23556 |
| cpu-burn | on | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 0.89 | 346 / 906 / 16303 | 0 / 0 / 0 | 13 / 379 / 15742 |
| cpu-burn | on | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 1.75 | 336 / 958 / 9531 | 0 / 0 / 0 | 12 / 676 / 9107 |
| cpu-burn | on | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=2 warm=22 | 1.78 | 475 / 10740 / 10997 | 0 / 0 / 0 | 20 / 10361 / 10591 |
| cpu-burn | on | 8 | 24 | 24 | 0 | 0 | 0 | 0 | warm=24 | 7.83 | 815 / 1378 / 1428 | 299 / 586 / 617 | 330 / 754 / 791 |

上限: gateway `[capacity] max_concurrency = 8`、`max_queue = 32`、`queue_timeout_seconds = 10`、revision の max_concurrency は上表の条件。総 request 数は並列度によらず固定（VM を飽和させないため）。
失敗の内訳: hello pool=cold c=8: Host.QuotaWaitTimeout=12; http-axum pool=cold c=8: Host.QuotaWaitTimeout=7; cpu-burn pool=cold c=8: Host.QuotaWaitTimeout=11

## 3. 資源原価

### 3.1 休止中（pool）の環境 1 つあたり

| sample | 窓 s | env | 要求 memory MiB | VMM state | VMM RSS MiB（始 → 終） | cgroup memory.current MiB（始 → 終） | memory.peak MiB | anon / file MiB | VMM CPU tick 増分 | cgroup CPU usec 増分 | host disk（env dir + jail、重複除く）MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|
| hello | 60 | `env_01m2s8e0rs14xcjt1a4x4xkb55` | 256 | S | 42.3 → 42.3 | 44.1 → 44.1 | 44.7 | 40.1 / 3.3 | 0 | 0 | 351.8 |
| http-axum | 60 | `env_01m2s95kfe073czqjwhqdpk760` | 256 | S | 42.5 → 42.5 | 44.9 → 44.7 | 45 | 40.3 / 3.3 | 0 | 0 | 351.8 |
| cpu-burn | 60 | `env_01m2s9zdawfvga8p7a23kgs01g` | 256 | S | 42.4 → 42.4 | 44.7 → 44.6 | 44.7 | 40.2 / 3.3 | 0 | 0 | 351.8 |

- hello: gateway RSS 36.4 → 36.5 MiB、gateway CPU tick 増分 12（CLK_TCK 100）、host MemAvailable 7302 → 7307.9 MiB
- http-axum: gateway RSS 35.3 → 35.3 MiB、gateway CPU tick 増分 14（CLK_TCK 100）、host MemAvailable 7291.2 → 7296.5 MiB
- cpu-burn: gateway RSS 36.9 → 36.9 MiB、gateway CPU tick 増分 13（CLK_TCK 100）、host MemAvailable 7293.2 → 7305.9 MiB

### 3.2 環境の寿命全体（provider が teardown 時に記録する cgroup の値。cold と warm の全環境）

| sample | 環境数 | memory.peak MiB p50 / p95 / max | CPU usage ms p50 / p95 / max | throttled ms p50 / p95 | throttled period 比 p50 / p95 | OOM kill |
|---|---|---|---|---|---|---|
| hello | 113 | 44.5 / 45.1 / 47 | 6014 / 8370 / 9106 | 5765 / 8033 | 0.98 / 1 | 0 |
| http-axum | 118 | 45 / 45.5 / 47 | 3974 / 9877 / 11262 | 3878 / 9448 | 0.99 / 1 | 0 |
| cpu-burn | 115 | 44.5 / 45 / 46.6 | 5560 / 7026 / 18000 | 5455 / 6706 | 0.98 / 1 | 0 |

### 3.3 gateway（host 側 bridge session）と pool の規模

| sample | 時点 | pool 内の環境 | gateway RSS MiB | host MemAvailable MiB |
|---|---|---|---|---|
| hello | gateway-empty | 0 | 34 | 7325.8 |
| hello | gateway-empty | 0 | 34 | 7320.9 |
| hello | gateway-empty | 0 | 34.2 | 7312.6 |
| hello | gateway-empty | 0 | 33.3 | 7327.5 |
| hello | gateway-empty | 0 | 33.3 | 7325 |
| hello | warm-gateway-empty | 0 | 33.1 | 7354.6 |
| hello | idle-start | 1 | 36.4 | 7302 |
| hello | idle-end | 1 | 36.5 | 7307.9 |
| hello | warm-after-sweep | 4 | 46.7 | 7166.7 |
| http-axum | gateway-empty | 0 | 34.4 | 7356.1 |
| http-axum | gateway-empty | 0 | 32.5 | 7346.3 |
| http-axum | gateway-empty | 0 | 33.9 | 7348.2 |
| http-axum | gateway-empty | 0 | 32.4 | 7338.5 |
| http-axum | gateway-empty | 0 | 33.4 | 7350.4 |
| http-axum | warm-gateway-empty | 0 | 32 | 7349.1 |
| http-axum | idle-start | 1 | 35.3 | 7291.2 |
| http-axum | idle-end | 1 | 35.3 | 7296.5 |
| http-axum | warm-after-sweep | 4 | 43.9 | 7167.5 |
| cpu-burn | gateway-empty | 0 | 34.4 | 7348.8 |
| cpu-burn | gateway-empty | 0 | 33.1 | 7350.2 |
| cpu-burn | gateway-empty | 0 | 33.3 | 7350 |
| cpu-burn | gateway-empty | 0 | 32.4 | 7342 |
| cpu-burn | gateway-empty | 0 | 34 | 7344.9 |
| cpu-burn | warm-gateway-empty | 0 | 33.5 | 7351.7 |
| cpu-burn | idle-start | 1 | 36.9 | 7293.2 |
| cpu-burn | idle-end | 1 | 36.9 | 7305.9 |
| cpu-burn | warm-after-sweep | 4 | 43.2 | 7178 |

- hello sweep 後に pool に残った環境 4（上限 max_idle_per_revision = 8）: cgroup memory.current p50 44.7 MiB / max 45 MiB、VMM RSS p50 42.2 MiB、disk p50 351.7 MiB
- http-axum sweep 後に pool に残った環境 4（上限 max_idle_per_revision = 8）: cgroup memory.current p50 45.1 MiB / max 45.3 MiB、VMM RSS p50 42.6 MiB、disk p50 351.7 MiB
- cpu-burn sweep 後に pool に残った環境 4（上限 max_idle_per_revision = 8）: cgroup memory.current p50 44.4 MiB / max 44.9 MiB、VMM RSS p50 42.1 MiB、disk p50 351.7 MiB

### 3.4 disk と転送量

| sample | scenario | function drive bytes p50 | scratch drive 作成 ms p50 / p95（cold） | request body bytes p50 | response body bytes p50 | response header bytes p50 |
|---|---|---|---|---|---|---|
| hello | cold | 10485760 | 5 / 9 | 16 | 84 | 259 |
| hello | warm | 10485760 | - / - | 16 | 84 | 259 |
| http-axum | cold | 10485760 | 5 / 46 | 0 | 2 | 267 |
| http-axum | warm | 10485760 | - / - | 0 | 2 | 267 |
| cpu-burn | cold | 10485760 | 9 / 88 | 15 | 70 | 259 |
| cpu-burn | warm | 10485760 | - / - | 15 | 70 | 259 |

guest の network 転送量: egress none（NIC 0、`env.network_interfaces`）のため 0。client ↔ gateway は loopback の HTTP で、上表の bytes がそのすべて。gateway ↔ guest の vsock の byte 数は計測していない（未測定）。

## 4. RFC 仮目標との比較

| 目標 | 判定 | 測定値 | 理由 |
|---|---|---|---|
| cold first response: small Rust function, image cache hit, p95 <= 3 s (RFC §19) | 未達 | `{"sample":"hello","n":20,"failure_rate":0,"client_p95_ms":16284,"client_p99_ms":16579,"environment_boot_p95_ms":14311,"runtime_init_p95_ms":1284,"cgroup_throttled_period_ratio_p50":0.98}` | p95 above 3000 ms on this host; see environment_boot_ms and the cgroup throttling ratio for where the time goes |
| cold first response, image cache miss: reported separately (RFC §19) | 記録のみ（目標値なし） | `[{"sample":"hello","n":5,"client_p95_ms":7728,"client_max_ms":7728,"failure_rate":0},{"sample":"http-axum","n":5,"client_p95_ms":11772,"client_max_ms":11772,"failure_rate":0},{"sample":"cpu-burn","n":5,"client_p95_ms":8887,"client_max_ms":8887,"failure_rate":0}]` | RFC gives no number for the miss case; recorded so it is not mixed into the hit percentiles |
| warm platform added latency p95 <= 20 ms, user handler excluded (RFC §19) | 未達（hello 未達、http-axum 達成、cpu-burn 未達。host 計測） | `[{"sample":"hello","n":20,"start_kinds":{"warm":20},"failure_rate":0,"host_platform_p95_ms":49,"client_platform_p95_ms":834},{"sample":"http-axum","n":20,"start_kinds":{"warm":20},"failure_rate":0,"host_platform_p95_ms":10,"client_platform_p95_ms":32},{"sample":"cpu-burn","n":20,"start_kinds":{"warm":20},"failure_rate":0,"host_platform_p95_ms":48,"client_platform_p95_ms":179}]` | judged per sample on host_platform p95; host_platform = total_ms - handler_ms (queue, resume, readiness, response, bookkeeping) over every attempt of the warm group including any that fell back to cold; client_platform adds loopback HTTP and curl. RFC asks for same-region under a set load; this is loopback on one nested host, sequential |
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
