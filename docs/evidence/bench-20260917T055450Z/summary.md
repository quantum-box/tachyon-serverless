# Benchmark bench-20260917T055450Z（PLT-4647）

**nested virtualization 上の aarch64 の記録であり、bare metal の x86_64 を代表しない。SLA・販売価格の根拠にしない。** 読み方と再実行は `docs/benchmark.md`。

## 条件

| 項目 | 値 |
|---|---|
| commit | `400bb45b0af5338ec1953a9b73a39f01bfe3b8de`（worktree dirty: false） |
| host | Linux lima-tsls-kvm 7.0.0-31-generic #31-Ubuntu SMP PREEMPT_DYNAMIC Sat Aug  1 03:33:01 UTC 2026 aarch64 GNU/Linux |
| CPU / memory | 4 vCPU（vendor Apple、model 0x000）/ MemTotal 8103308 KiB |
| physical host | Apple M4 (macOS, Darwin 25.6.0) -> Lima 'tsls-kvm' vmType vz with nested virtualization, 4 vCPU / 8 GiB guest; the Mac is a shared developer machine (see physical-host-load.tsv) |
| KVM / nested | rw / nested virtualization = true（virtualization: apple） |
| Firecracker / jailer | Firecracker v1.17.0 / Jailer v1.17.0 |
| guest kernel sha256 | `e3544b10603acbf3db492cb52e000d22ba202cb4b63b9add027565683e11c591`（firecracker-ci/v1.15/aarch64/vmlinux-6.1.155） |
| rootfs sha256 | `7fbb184ee5fdbcd3ce1bae25c5cb1647db482f0373afa84f24455173922da75b`（67108864 bytes、bridge 4963152 bytes） |
| profile | firecracker microVM, jailer enabled, host cgroup v2 mode = required, egress none、256 MiB、500 m、timeout 30 s、revision max_concurrency product default (4) |
| gateway | release build、root via sudo (jailer + host cgroup required)、config `gateway-cold.toml` / `gateway-warm.toml`（`[pool] enabled = true`） |
| 負荷 | fresh host 5 回 / cold 20 回 / warm 20 回（間隔 250 ms）/ sweep 1,2,4,8 並列 × 各 24 request（cold / warm）/ idle 60 s |
| payload | hello=POST :invoke {"name":"bench"} http-axum=GET /http/ (router answers ok) cpu-burn=POST :invoke {"seconds":0.2} |
| client / seed | curl on the same host over loopback, one request per process, no keep-alive。none: payloads are fixed and requests are issued in a fixed order; nothing is randomised |
| host の混み具合 | VM 内の固定 CPU loop（ms、3 回）: start 112/118/119、before-hello 117/115/122、before-http-axum 91/83/82、before-cpu-burn 399/607/401、end 82/82/84。物理 host の 1 分 load average: p50 6.54 / max 31.50（n=69、`physical-host-load.tsv`） |

percentile は nearest-rank。client の分布は **失敗を含む全 attempt**、内訳の分布はその値を持つ attempt（n を併記）。外れ値は除外していない。生データは `attempts.jsonl`（1 request 1 行）と `invocations.jsonl`。

## 1. first response（ms、p50 / p95 / p99）

| sample | scenario | n | 失敗率 | start kind | client | queue | boot | init | resume | readiness | handler | host platform（total−handler） | client−total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| hello | fresh-miss | 5 | 0 | cold=5 | 6158 / 7609 / 7609 | 0 / 0 / 0 | 5438 / 6836 / 6836 | 516 / 571 / 571 | - / - / - | - / - / - | 122 / 169 / 169 | 5943 / 7416 / 7416 | 139 / 144 / 144 |
| hello | cold | 20 | 0 | cold=20 | 5817 / 6037 / 6071 | 0 / 0 / 0 | 5102 / 5315 / 5374 | 501 / 566 / 578 | - / - / - | - / - / - | 112 / 173 / 191 | 5611 / 5835 / 5890 | 85 / 133 / 187 |
| hello | warm-prime | 1 | 0 | cold=1 | 5765 / 5765 / 5765 | 0 / 0 / 0 | 5180 / 5180 / 5180 | 439 / 439 / 439 | - / - / - | - / - / - | 159 / 159 / 159 | 5628 / 5628 / 5628 | -22 / -22 / -22 |
| hello | warm | 20 | 0 | warm=20 | 56 / 75 / 95 | 0 / 0 / 0 | - / - / - | - / - / - | 1 / 1 / 1 | 3 / 14 / 18 | 42 / 48 / 72 | 7 / 17 / 24 | 6 / 9 / 9 |
| http-axum | fresh-miss | 5 | 0 | cold=5 | 5908 / 6128 / 6128 | 0 / 0 / 0 | 5163 / 5333 / 5333 | 514 / 587 / 587 | - / - / - | - / - / - | 109 / 114 / 114 | 5686 / 5929 / 5929 | 87 / 191 / 191 |
| http-axum | cold | 20 | 0 | cold=20 | 6502 / 9430 / 9837 | 0 / 0 / 0 | 5551 / 7776 / 8692 | 555 / 819 / 1084 | - / - / - | - / - / - | 122 / 215 / 280 | 6184 / 8874 / 9529 | 130 / 234 / 276 |
| http-axum | warm-prime | 1 | 0 | cold=1 | 5744 / 5744 / 5744 | 0 / 0 / 0 | 5128 / 5128 / 5128 | 513 / 513 / 513 | - / - / - | - / - / - | 116 / 116 / 116 | 5650 / 5650 / 5650 | -22 / -22 / -22 |
| http-axum | warm | 20 | 0 | warm=20 | 70 / 112 / 267 | 0 / 0 / 0 | - / - / - | - / - / - | 1 / 3 / 5 | 3 / 15 / 18 | 47 / 102 / 192 | 8 / 31 / 40 | 7 / 21 / 65 |
| cpu-burn | fresh-miss | 5 | 0 | cold=5 | 6925 / 14106 / 14106 | 0 / 0 / 0 | 6036 / 12827 / 12827 | 522 / 592 / 592 | - / - / - | - / - / - | 320 / 400 / 400 | 6541 / 13475 / 13475 | 81 / 231 / 231 |
| cpu-burn | cold | 20 | 0 | cold=20 | 6944 / 12250 / 14200 | 0 / 0 / 0 | 5756 / 10490 / 12840 | 499 / 1120 / 1197 | - / - / - | - / - / - | 319 / 481 / 533 | 6261 / 11606 / 13649 | 82 / 294 / 550 |
| cpu-burn | warm-prime | 1 | 0 | cold=1 | 7225 / 7225 / 7225 | 0 / 0 / 0 | 5769 / 5769 / 5769 | 1106 / 1106 / 1106 | - / - / - | - / - / - | 370 / 370 / 370 | 6884 / 6884 / 6884 | -29 / -29 / -29 |
| cpu-burn | warm | 20 | 0 | warm=20 | 278 / 311 / 340 | 0 / 0 / 0 | - / - / - | - / - / - | 1 / 1 / 2 | 3 / 3 / 13 | 268 / 301 / 310 | 6 / 17 / 26 | 4 / 17 / 21 |

- `fresh-miss`: gateway 停止 → data_dir と workdir を削除 → `drop_caches` → gateway 起動 → create / deploy → 最初の invoke（kernel・rootfs・VMM・artifact が page cache に無い）。n は fresh host の試行数で、20 未満のため p95 / p99 は最大値に近い。
- `cold`: 同じ gateway、pool 無効。page cache は温まっているが **function drive と scratch drive は環境ごとに毎回作る**（drive の cache は実装に無い）。
- `warm`: pool 有効の gateway で 1 回 prime した後の連続 invoke。warm にならなかった attempt も除外せず含める（start kind の列）。
- boot / init は cold attempt だけ、resume / readiness は warm attempt だけが持つ。
- 2 attempt になった invocation は無い。
- 失敗した attempt は無い。

warm のうち start kind が warm の attempt だけ（参考）:

| sample | n | client | host platform | client platform（client−handler） |
|---|---|---|---|---|
| hello | 20 | 56 / 75 / 95 | 7 / 17 / 24 | 13 / 23 / 31 |
| http-axum | 20 | 70 / 112 / 267 | 8 / 31 / 40 | 15 / 51 / 75 |
| cpu-burn | 20 | 278 / 311 / 340 | 6 / 17 / 26 | 10 / 30 / 47 |

gateway の起動（start → readyz 200）: fresh host 500 / 2080 / 2080（n=15）、page cache が温まった状態 493 / 499 / 499（n=3）。deploy（CLI、upload + revision ready）: 277 / 481 / 481（n=15）。

## 2. 同時実行（sweep）

| sample | pool | 並列 | n | 成功 | 失敗率 | 429 | 503 | 504 | start kind | 成功/秒 | client p50 / p95 / p99 | queue p50 / p95 / p99 | host platform p50 / p95 / p99 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| hello | off | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.17 | 5838 / 6475 / 8024 | 0 / 0 / 0 | 5599 / 6170 / 7832 |
| hello | off | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.3 | 6381 / 7356 / 8330 | 0 / 0 / 0 | 6133 / 7112 / 8020 |
| hello | off | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.29 | 11825 / 27970 / 28224 | 0 / 0 / 0 | 11410 / 27626 / 27919 |
| hello | off | 8 | 24 | 20 | 0.167 | 0 | 0 | 4 | cold=20 none=4 | 0.42 | 10126 / 19429 / 19910 | 1876 / 9179 / 9589 | 10432 / 19088 / 19665 |
| hello | on | 1 | 24 | 24 | 0 | 0 | 0 | 0 | warm=24 | 14.75 | 42 / 73 / 74 | 0 / 0 / 0 | 4 / 5 / 11 |
| hello | on | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 3.33 | 45 / 78 / 6489 | 0 / 0 / 0 | 4 / 11 / 6409 |
| hello | on | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=2 warm=22 | 3.55 | 50 / 6388 / 6398 | 0 / 0 / 0 | 5 / 6249 / 6308 |
| hello | on | 8 | 24 | 24 | 0 | 0 | 0 | 0 | cold=2 warm=22 | 3.53 | 142 / 6439 / 6467 | 57 / 99 / 118 | 77 / 6285 / 6373 |
| http-axum | off | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.16 | 5926 / 8581 / 8691 | 0 / 0 / 0 | 5707 / 8060 / 8508 |
| http-axum | off | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.31 | 6157 / 8354 / 8619 | 0 / 0 / 0 | 5882 / 8170 / 8431 |
| http-axum | off | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.5 | 7730 / 8966 / 9193 | 0 / 0 / 0 | 7431 / 8652 / 8777 |
| http-axum | off | 8 | 24 | 22 | 0.083 | 0 | 0 | 2 | cold=22 none=2 | 0.42 | 14611 / 22369 / 22503 | 7763 / 8945 / 9265 | 15025 / 21793 / 21814 |
| http-axum | on | 1 | 24 | 24 | 0 | 0 | 0 | 0 | warm=24 | 1.85 | 390 / 887 / 932 | 0 / 0 / 0 | 13 / 74 / 98 |
| http-axum | on | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 1.13 | 127 / 855 / 19710 | 0 / 0 / 0 | 9 / 206 / 19436 |
| http-axum | on | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=2 warm=22 | 2.3 | 92 / 9839 / 9859 | 0 / 0 / 0 | 7 / 9661 / 9678 |
| http-axum | on | 8 | 24 | 22 | 0.083 | 0 | 0 | 0 | none=2 warm=22 | 0.7 | 167 / 30110 / 30130 | 77 / 115 / 121 | 86 / 124 / 129 |
| cpu-burn | off | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.13 | 6053 / 12967 / 13852 | 0 / 0 / 0 | 5544 / 12263 / 13372 |
| cpu-burn | off | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.32 | 6123 / 6820 / 6883 | 0 / 0 / 0 | 5730 / 6329 / 6499 |
| cpu-burn | off | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=24 | 0.44 | 7575 / 15052 / 15145 | 0 / 0 / 0 | 7013 / 14174 / 14675 |
| cpu-burn | off | 8 | 24 | 21 | 0.125 | 0 | 0 | 3 | cold=21 none=3 | 0.41 | 14186 / 23350 / 23746 | 7639 / 9570 / 9716 | 15765 / 22898 / 23251 |
| cpu-burn | on | 1 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 1.22 | 331 / 434 / 10332 | 0 / 0 / 0 | 11 / 25 / 9966 |
| cpu-burn | on | 2 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 2.54 | 279 / 295 / 6290 | 0 / 0 / 0 | 4 / 21 / 6004 |
| cpu-burn | on | 4 | 24 | 24 | 0 | 0 | 0 | 0 | cold=2 warm=22 | 3.2 | 282 / 5954 / 5969 | 0 / 0 / 0 | 4 / 5675 / 5676 |
| cpu-burn | on | 8 | 24 | 24 | 0 | 0 | 0 | 0 | cold=1 warm=23 | 3.6 | 548 / 747 / 6026 | 278 / 468 / 471 | 290 / 483 / 5745 |

上限: gateway `[capacity] max_concurrency = 8`、`max_queue = 32`、`queue_timeout_seconds = 10`、revision の max_concurrency は上表の条件。総 request 数は並列度によらず固定（VM を飽和させないため）。
失敗の内訳: hello pool=cold c=8: Host.QueueTimeout=4; http-axum pool=cold c=8: Host.QueueTimeout=2; http-axum pool=warm c=8: Host.EnvironmentBootFailed=2; cpu-burn pool=cold c=8: Host.QueueTimeout=3

## 3. 資源原価

### 3.1 休止中（pool）の環境 1 つあたり

| sample | 窓 s | env | 要求 memory MiB | VMM state | VMM RSS MiB（始 → 終） | cgroup memory.current MiB（始 → 終） | memory.peak MiB | anon / file MiB | VMM CPU tick 増分 | cgroup CPU usec 増分 | host disk（env dir + jail、重複除く）MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|
| hello | 60 | `env_01m2pzd8cnhpmkte3garbbamr1` | 256 | S | 38.6 → 38.6 | 41 → 40.8 | 41 | 36.4 / 3.3 | 0 | 0 | 349.6 |
| http-axum | 60 | `env_01m2q00ecc7cets75awg23zfhn` | 256 | S | 38.8 → 38.8 | 41.2 → 41 | 41.5 | 36.6 / 3.3 | 0 | 0 | 349.6 |
| cpu-burn | 60 | `env_01m2q0qmjrxrcds35fzpnpwz1j` | 256 | S | 38.6 → 38.6 | 41 → 40.4 | 41 | 36.5 / 3.3 | 0 | 0 | 349.6 |

- hello: gateway RSS 22.3 → 22.4 MiB、gateway CPU tick 増分 3（CLK_TCK 100）、host MemAvailable 7305.5 → 7310.7 MiB
- http-axum: gateway RSS 22.4 → 22.4 MiB、gateway CPU tick 増分 3（CLK_TCK 100）、host MemAvailable 7309.4 → 7318.6 MiB
- cpu-burn: gateway RSS 23.8 → 23.8 MiB、gateway CPU tick 増分 3（CLK_TCK 100）、host MemAvailable 7313.5 → 7323.2 MiB

### 3.2 環境の寿命全体（provider が teardown 時に記録する cgroup の値。cold と warm の全環境）

| sample | 環境数 | memory.peak MiB p50 / p95 / max | CPU usage ms p50 / p95 / max | throttled ms p50 / p95 | throttled period 比 p50 / p95 | OOM kill |
|---|---|---|---|---|---|---|
| hello | 123 | 41 / 41.4 / 43.4 | 3229 / 6277 / 14024 | 3159 / 6113 | 0.99 / 1 | 0 |
| http-axum | 123 | 41.1 / 41.6 / 43.2 | 3342 / 5260 / 13326 | 3253 / 4901 | 0.99 / 1 | 0 |
| cpu-burn | 124 | 40.9 / 41.4 / 43 | 3507 / 7111 / 15284 | 3370 / 6844 | 0.99 / 1 | 0 |

### 3.3 gateway（host 側 bridge session）と pool の規模

| sample | 時点 | pool 内の環境 | gateway RSS MiB | host MemAvailable MiB |
|---|---|---|---|---|
| hello | gateway-empty | 0 | 19.5 | 7381.2 |
| hello | gateway-empty | 0 | 19.5 | 7380.4 |
| hello | gateway-empty | 0 | 19.5 | 7375.5 |
| hello | gateway-empty | 0 | 19.5 | 7367 |
| hello | gateway-empty | 0 | 21 | 7374.1 |
| hello | warm-gateway-empty | 0 | 19.6 | 7356.6 |
| hello | idle-start | 1 | 22.3 | 7305.5 |
| hello | idle-end | 1 | 22.4 | 7310.7 |
| hello | warm-after-sweep | 6 | 38.9 | 7112.2 |
| http-axum | gateway-empty | 0 | 19.5 | 7349.2 |
| http-axum | gateway-empty | 0 | 21.3 | 7341.1 |
| http-axum | gateway-empty | 0 | 19.5 | 7349.7 |
| http-axum | gateway-empty | 0 | 19.5 | 7350.2 |
| http-axum | gateway-empty | 0 | 19.5 | 7350 |
| http-axum | warm-gateway-empty | 0 | 19.6 | 7358.9 |
| http-axum | idle-start | 1 | 22.4 | 7309.4 |
| http-axum | idle-end | 1 | 22.4 | 7318.6 |
| http-axum | warm-after-sweep | 4 | 37 | 7193.9 |
| cpu-burn | gateway-empty | 0 | 19.5 | 7362.6 |
| cpu-burn | gateway-empty | 0 | 19.5 | 7368.3 |
| cpu-burn | gateway-empty | 0 | 21.4 | 7366.4 |
| cpu-burn | gateway-empty | 0 | 21.5 | 7357.3 |
| cpu-burn | gateway-empty | 0 | 19.5 | 7364.1 |
| cpu-burn | warm-gateway-empty | 0 | 21 | 7367.9 |
| cpu-burn | idle-start | 1 | 23.8 | 7313.5 |
| cpu-burn | idle-end | 1 | 23.8 | 7323.2 |
| cpu-burn | warm-after-sweep | 5 | 34.2 | 7162.3 |

- hello sweep 後に pool に残った環境 6（上限 max_idle_per_revision = 8）: cgroup memory.current p50 40.7 MiB / max 41.4 MiB、VMM RSS p50 38.6 MiB、disk p50 349.6 MiB
- http-axum sweep 後に pool に残った環境 4（上限 max_idle_per_revision = 8）: cgroup memory.current p50 40.6 MiB / max 41.4 MiB、VMM RSS p50 38.8 MiB、disk p50 349.6 MiB
- cpu-burn sweep 後に pool に残った環境 5（上限 max_idle_per_revision = 8）: cgroup memory.current p50 41.1 MiB / max 41.3 MiB、VMM RSS p50 38.6 MiB、disk p50 349.6 MiB

### 3.4 disk と転送量

| sample | scenario | function drive bytes p50 | scratch drive 作成 ms p50 / p95（cold） | request body bytes p50 | response body bytes p50 | response header bytes p50 |
|---|---|---|---|---|---|---|
| hello | cold | 10485760 | 2 / 4 | 16 | 84 | 259 |
| hello | warm | 10485760 | - / - | 16 | 84 | 259 |
| http-axum | cold | 10485760 | 3 / 5 | 0 | 2 | 267 |
| http-axum | warm | 10485760 | - / - | 0 | 2 | 267 |
| cpu-burn | cold | 10485760 | 3 / 12 | 15 | 70 | 259 |
| cpu-burn | warm | 10485760 | - / - | 15 | 70 | 259 |

guest の network 転送量: egress none（NIC 0、`env.network_interfaces`）のため 0。client ↔ gateway は loopback の HTTP で、上表の bytes がそのすべて。gateway ↔ guest の vsock の byte 数は計測していない（未測定）。

## 4. RFC 仮目標との比較

| 目標 | 判定 | 測定値 | 理由 |
|---|---|---|---|
| cold first response: small Rust function, image cache hit, p95 <= 3 s (RFC §19) | 未達 | `{"sample":"hello","n":20,"failure_rate":0,"client_p95_ms":6037,"client_p99_ms":6071,"environment_boot_p95_ms":5315,"runtime_init_p95_ms":566,"cgroup_throttled_period_ratio_p50":0.99}` | p95 above 3000 ms on this host; see environment_boot_ms and the cgroup throttling ratio for where the time goes |
| cold first response, image cache miss: reported separately (RFC §19) | 記録のみ（目標値なし） | `[{"sample":"hello","n":5,"client_p95_ms":7609,"client_max_ms":7609,"failure_rate":0},{"sample":"http-axum","n":5,"client_p95_ms":6128,"client_max_ms":6128,"failure_rate":0},{"sample":"cpu-burn","n":5,"client_p95_ms":14106,"client_max_ms":14106,"failure_rate":0}]` | RFC gives no number for the miss case; recorded so it is not mixed into the hit percentiles |
| warm platform added latency p95 <= 20 ms, user handler excluded (RFC §19) | 未達（hello 達成、http-axum 未達、cpu-burn 達成。host 計測） | `[{"sample":"hello","n":20,"start_kinds":{"warm":20},"failure_rate":0,"host_platform_p95_ms":17,"client_platform_p95_ms":23},{"sample":"http-axum","n":20,"start_kinds":{"warm":20},"failure_rate":0,"host_platform_p95_ms":31,"client_platform_p95_ms":51},{"sample":"cpu-burn","n":20,"start_kinds":{"warm":20},"failure_rate":0,"host_platform_p95_ms":17,"client_platform_p95_ms":30}]` | judged per sample on host_platform p95; host_platform = total_ms - handler_ms (queue, resume, readiness, response, bookkeeping) over every attempt of the warm group including any that fell back to cold; client_platform adds loopback HTTP and curl. RFC asks for same-region under a set load; this is loopback on one nested host, sequential |
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
