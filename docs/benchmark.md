# ベンチマーク: first response・同時実行・資源原価（PLT-4647）

- 対象: `scripts/kvm/bench.sh`（計測）、`scripts/kvm/bench-sample.sh`（host 側の資源 snapshot、root）、`scripts/kvm/bench-report.sh`（集計。offline で再実行できる）
- 関連: [kvm.md](kvm.md)（§3.5 gateway、§3.6 host cgroup / jailer、§3.7 warm 再利用、§5 Lima）、[adr/0001](adr/0001-execution-provider-firecracker-first.md)「残る測定」M2〜M4・M7、[inventory-tachyon-apps.md](inventory-tachyon-apps.md) §5・§6、[acceptance.md](acceptance.md)「PLT-4647」
- 状態: 1 回の記録がある（`docs/evidence/bench-20260917T055450Z/`）。**Apple M4 上の Lima VM（nested virtualization）の aarch64 だけ**で、x86_64・bare metal・専用の新品 host では未測定。

**ここにある数値は SLA でも販売価格の根拠でもない。** nested virtualization のオーバーヘッド（とくに guest の serial console と KVM の二重化）を含み、bare metal の x86_64 の値を代表しない。RFC の仮目標（§6）と比べた判定も、この host での判定である。

## 1. 目的

「VM の起動時間の一部」ではなく、**利用者が応答を受け取るまでの時間**と、そのために host が払う資源を同じ条件でまとめて残す。起動の内訳（queue / boot / init / handler）、同時実行時の拒否と待ち、環境 1 つあたりの固定 memory、休止中の環境が持ち続ける資源、disk と転送量を 1 回の実行で記録し、生データから表を作り直せるようにする。

## 2. 何を測るか

対象は Rust サンプル 3 種。payload は固定で、乱数は使わない（seed は無い）。

| sample | リクエスト | handler がすること |
|---|---|---|
| `examples/hello` | `POST /v1/functions/{id}/invoke?revision_id=...`、`{"name":"bench"}` | JSON を返す |
| `examples/http-axum` | `GET /v1/functions/{id}/http/`（`x-tachyon-revision-id`） | axum router が `ok` を返す |
| `examples/cpu-burn` | `POST .../invoke`、`{"seconds":0.2}`（`BENCH_CPU_BURN_SECONDS`） | 0.2 秒の CPU busy loop |

revision は baseline profile（`docs/inventory-tachyon-apps.md` §6）と同じ 256 MiB・500 m（1 vCPU）・timeout 30 s、egress none。gateway は `config/gateway.firecracker.toml`（`profile = "production"`、**jailer 有効、host cgroup v2 `mode = "required"`**）の `data_dir` だけを変えたコピーで、root（sudo）で動かす。gateway と CLI は release build。

| scenario | 手順 | 何が cache されているか |
|---|---|---|
| `fresh-miss`（fresh host / image cache miss） | gateway 停止 → `data_dir` と workdir（`.kvm/run`）を削除 → `sync; echo 3 > /proc/sys/vm/drop_caches` → gateway 起動 → `functions create` / `deploy` → 最初の invoke。sample ごとに `BENCH_FRESH_TRIALS`（既定 5）回 | 何も無い。台帳・artifact は空から作り、kernel・rootfs・firecracker / jailer・gateway のバイナリ・artifact は page cache に無い |
| `cold`（cache hit、pool 無効） | 同じ gateway で `BENCH_COLD_N`（既定 20）回、間隔なしで順に invoke | page cache に kernel・rootfs・artifact・VMM がある。preflight の digest は gateway が memo 化済み。**function drive（`mkfs.ext4 -d`）と scratch drive（`fallocate` + `mkfs.ext4`）は環境ごとに毎回作る**。この実装に drive や VM image の cache は無い |
| `warm-prime` / `warm`（pool 有効） | gateway を `[pool] enabled = true`（`max_idle_per_revision = 8`、`idle_ttl_seconds = 900`）で再起動し、1 回 prime（cold）した後 `BENCH_WARM_N`（既定 20）回、`BENCH_WARM_GAP_MS`（既定 250 ms）間隔で invoke | 前の invoke の環境が休止（`PATCH /vm Paused`）して pool にあり、再開（`Resumed`）→ `Ping` / `Pong` → dispatch。profile は production のまま（Firecracker は `idle_quiesce` / `idle_resume` を `Supported` と申告するので計測用 switch は要らない） |
| `sweep-cold` / `sweep-warm` | 並列 client 数 `BENCH_SWEEP_LEVELS`（既定 1 2 4 8）ごとに、合計 `BENCH_SWEEP_REQUESTS`（既定 24）request を client に分けて間隔なしで投げる。pool 無効と有効の 2 通り | 上と同じ。総数を並列度によらず固定して VM を飽和させない |
| 資源 | pool に休止環境がある状態で `bench-sample.sh` を 2 回（`BENCH_IDLE_SECONDS`、既定 60 秒あけて）、sweep の後に 1 回。gateway 起動直後（環境 0）にも 1 回 | — |

同時実行の上限は gateway の `[capacity] max_concurrency = 8`、`max_queue = 32`、`queue_timeout_seconds = 10` と、revision の `max_concurrency`（既定 4、`BENCH_REVISION_MAX_CONCURRENCY` で変更）。上限を超えた request の 429 / 503 / 504 もそのまま記録する。

## 3. 実行

前提: `docs/kvm.md` §3.2 の bootstrap が済んだ Linux/KVM host、passwordless sudo、`/dev/kvm` rw、`jq` `curl` `debugfs`（e2fsprogs）。**他の gateway / firecracker / jailer が動いている host では始めない**（preflight が拒否する）。root fs の使用率が `BENCH_MAX_DISK_PCT`（既定 60%）以上なら始めず、sample の間でも超えたら止める。

```sh
scripts/kvm/bootstrap.sh              # 初回だけ（kvm.md §3.2）
scripts/kvm/bench.sh                  # 既定: 3 sample、fresh 5 / cold 20 / warm 20 / sweep 1,2,4,8 x 24 / idle 60 s
scripts/kvm/bench-report.sh docs/evidence/bench-<UTC>   # 表だけ作り直す（どの OS でも。jq だけ使う）
```

git checkout でない tree（Lima VM に rsync したコピーなど）では `BENCH_COMMIT=<sha>`、`BENCH_DIRTY=false|true` を渡す。VM の中からは物理 host が見えないので `BENCH_HOST_NOTE` に書き、nested virtualization の判定（`systemd-detect-virt` が `none` 以外なら true）を `BENCH_NESTED` で上書きできる。

主な環境変数: `BENCH_SAMPLES`、`BENCH_FRESH_TRIALS`、`BENCH_COLD_N`、`BENCH_WARM_N`、`BENCH_WARM_GAP_MS`、`BENCH_SWEEP_LEVELS`、`BENCH_SWEEP_REQUESTS`、`BENCH_SWEEP_MODES`（`cold warm`）、`BENCH_IDLE_SECONDS`、`BENCH_MEMORY_MIB`、`BENCH_CPU_MILLIS`、`BENCH_TIMEOUT_SECONDS`、`BENCH_REVISION_MAX_CONCURRENCY`、`BENCH_CPU_BURN_SECONDS`、`BENCH_REQUEST_TIMEOUT_S`（90）、`BENCH_MAX_DISK_PCT`、`TSLS_SKIP_BUILD`、`TSLS_GATEWAY_CONFIG`（jailer 有効と cgroup required を preflight で確認する）、`EVIDENCE_ROOT`。

所要時間は 4 vCPU の nested host で約 35 分（cold の起動 1 回が 5〜10 秒かかるため、ほとんどが cold と sweep-cold）。物理 host が混んでいると数倍に伸び、起動 timeout が増える（§7）。

**物理 host の混み具合を記録する。** VM の中からは物理 host の負荷が見えない（Apple の hypervisor は steal time を出さない）。`bench.sh` は VM 内で固定の CPU loop の時間を `calibration.jsonl` に取る（開始時、sample ごと、終了時）。`BENCH_MAX_CALIBRATION_MS` を設定すると、開始時の値がそれを超えたとき計測を始めない。Lima のように外側の host が見える場合は、実行中に外側で 1 分 load average を取り、`physical-host-load.tsv`（`<UTC>\t{ l1 l5 l15 }`、macOS の `sysctl -n vm.loadavg` の形）として evidence に置いてから `bench-report.sh` を再実行すると summary に載る。

終了コード: 0 計測できた（失敗した request はデータであって終了コードにしない）/ 2 計測できなかった（preflight、build、gateway、deploy）/ 3 実行後に gateway・VMM・jail・cgroup・tap・環境ディレクトリが残った（`cleanup.txt`）。

## 4. 出力（`docs/evidence/bench-<UTC>/`）

| ファイル | 内容 |
|---|---|
| `summary.md` / `summary.json` | 表（§5 の規則で集計）、RFC 仮目標との比較、P0 条件との比較 |
| `metadata.json` / `profile/` | host（uname、CPU、vCPU、memory、KVM、nested virtualization、物理 host のメモ）、Firecracker / jailer の版、kernel / rootfs / bridge / 各 artifact の sha256 と大きさ、commit、config の sha256、profile、負荷条件、試行数、seed |
| `gateway-cold.toml` / `gateway-warm.toml` | 実際に使った設定 |
| `attempts.jsonl` | **1 request 1 行**（失敗・429 を含む）。client 計測（`client_ms`、TTFB、送受信 bytes、HTTP code、error code）と、`GET /v1/invocations/{id}` から取った status、start kind、attempt 数、`timings`、環境の証跡（function / scratch drive の bytes、scratch drive 作成 ms、cgroup の上限、jailed、NIC 数） |
| `invocations.jsonl` | `InvocationResponse` の生データ |
| `sweeps.jsonl` | sweep の段ごとの壁時計 |
| `resources.jsonl` | `bench-sample.sh` の snapshot（環境ごとの cgroup memory.current / peak / stat、CPU usage、VMM の RSS / HWM / tick / state、env dir と jail の bytes、gateway の RSS / tick、host MemAvailable） |
| `gateway-runs.jsonl` / `gateway-logs/` | gateway の起動ごとの start → readyz 時間とログ（provider の `cgroup stats at teardown` に環境の寿命全体の memory.peak と CPU が入る） |
| `deploys.jsonl` / `revision-*.json` | create / deploy の所要時間と revision の spec |
| `cleanup.txt` / `steps/` / `steps.json` | 実行後の残留検査と step ログ |

## 5. 集計の規則

- percentile は nearest-rank（`sorted[ceil(p/100 * n) - 1]`）。p50 / p95 / p99 を出す。n < 20 の group（fresh host）の p95 / p99 は最大値とほぼ同じ意味しかない。
- **client の分布は失敗を含む全 attempt**で取る。失敗率は別の列に出す。外れ値を捨てない。
- 内訳の分布はその値を持つ attempt だけで取り、n を出す。`environment_boot_ms` / `runtime_init_ms` は cold の attempt だけ、`resume_ms` / `readiness_ms` は warm の attempt だけが持つ。warm scenario で cold に落ちた attempt も warm scenario の行に含める（start kind の列で数を示す）。
- 定義（すべて host 計測、`AttemptTimings`）:
  - client = curl の `time_total`（同じ host の loopback、1 request 1 プロセス、keep-alive なし）
  - queue = `queue_wait_ms`（受付 → dispatch 開始）
  - boot = `environment_boot_ms`（環境作成開始 → bridge の `Hello`。drive 作成、cgroup、jailer、VMM の設定、guest kernel の起動を含む）
  - init = `runtime_init_ms`（`Hello` → `Ready`）
  - handler = `handler_ms`、response = `response_ms`、total = `total_ms`（受付 → 結果確定）
  - **host platform = total − handler**（queue・boot・init・resume・readiness・response と記帳。「基盤が足した時間」の host 側の値）
  - client platform = client − handler（上に loopback HTTP と curl を足したもの）
  - client − total = HTTP と curl の分。負になる attempt がある（smoke 実行では pool 有効の cold attempt で −35〜−40 ms。`total_ms` の確定が client への応答の送出より後になっていると読めるが、原因は調べていない）。
- 資源:
  - 固定 memory は host から見た値だけを使う: VMM プロセスの `VmRSS`（guest memory のうち触られた分と VMM 自身）と、環境 cgroup の `memory.current`（VMM + drive の page cache）/ `memory.peak`。要求値（`mem_size_mib`）は上限であって消費ではない。guest の中の内訳（kernel / bridge / 関数）は host から見えないので未測定。
  - bridge の host 側の費用は gateway の RSS を pool 内の環境数と並べて見る（gateway 起動直後の環境 0 と比べる）。guest 側の bridge は rootfs に入った static バイナリの大きさ（`metadata.json` の `runtime_bridge.bytes`）だけを記録する。
  - 休止中の原価は `BENCH_IDLE_SECONDS` の間の VMM の CPU tick（utime + stime）と cgroup の `usage_usec` の増分、RSS と `memory.current` の始点・終点。
  - disk は `function_drive_bytes`（artifact + 8 MiB）、`scratch_drive_bytes`（`ephemeral_storage_mib`、作成時に確保）、`du` で見た env dir と jail（hard link は 1 回だけ数える）。
  - 転送量は client ↔ gateway の HTTP の body / header bytes。egress none の guest に NIC は無い（`network_interfaces = 0`）ので guest の network 転送は 0。gateway ↔ guest の vsock は数えていない。

## 6. 結果（`docs/evidence/bench-20260917T055450Z/`）

条件: commit `400bb45`（rebase 前の branch の commit。計測スクリプトだけを足した commit で、スクリプトは rebase 後の `28aed1b` と同じ。application / provider のコードは `origin/main` `3704c80` と同じ）、Apple M4 上の Lima VM（vz、nested virtualization、4 vCPU / 8 GiB、Linux 7.0.0-31 aarch64）、Firecracker / jailer v1.17.0、guest kernel 6.1.155、256 MiB・500 m・jailer・cgroup required・egress none、gateway release build。全 714 request、失敗 11（すべて並列 8 の sweep）。表の全体は `summary.md`。計測した commit は `origin/main` `3704c80` を基点にしており、その後 main に入った PLT-4634（admission・autoscaler。`[capacity]` の semaphore を置き換え）、PLT-4636、PLT-4638 は含まない。とくに同時実行の拒否と queue の挙動（§6.2）はこれらで変わりうるが、**新しい main での再計測はしていない**。

物理 host（開発用の Mac）は他の作業と共用で、1 分 load average は p50 6.5 / 最大 31.5 だった（`physical-host-load.tsv`）。cpu-burn の開始時の calibration は他の sample の 4〜5 倍（399〜607 ms、他は 82〜122 ms）で、cpu-burn の cold の p95 / p99 と fresh host の最大値はその影響を含む。同じ日の 1 回目の実行（load average 40〜140）は失敗が多発したため途中で止めた（§7「物理 host の共用」に内容を記録）。

### 6.1 first response（client ms、p50 / p95 / p99。失敗を含む）

| sample | fresh host（cache miss、n=5） | cold（cache hit、n=20） | warm（n=20、全て warm） | warm の host platform（total − handler） |
|---|---|---|---|---|
| hello | 6158 / 7609 / 7609 | 5817 / 6037 / 6071 | 56 / 75 / 95 | 7 / 17 / 24 |
| http-axum | 5908 / 6128 / 6128 | 6502 / 9430 / 9837 | 70 / 112 / 267 | 8 / 31 / 40 |
| cpu-burn（0.2 s） | 6925 / 14106 / 14106 | 6944 / 12250 / 14200 | 278 / 311 / 340 | 6 / 17 / 26 |

cold の内訳（hello、p50 / p95）: queue 0 / 0、boot 5102 / 5315、init 501 / 566、handler 112 / 173、client − total 85 / 133。warm の内訳（hello）: resume 1 / 1、readiness 3 / 14、handler 42 / 48。

- cold の時間のほとんどは `environment_boot_ms`（drive 作成・jailer・VMM 設定・guest kernel の起動）。環境の cgroup の teardown 記録では、**CPU の throttled period 比が p50 0.99**（370 環境）で、boot 中ずっと 500 m の quota に当たっている。guest kernel の起動（serial console へのログ出力を含む）が nested virtualization で重く、その CPU が quota を使い切っていると読める。console を抑えた場合や quota を変えた場合の比較はしていない（§7）。scratch drive の作成は p50 2〜3 ms、function drive は 10 MiB（artifact + 8 MiB）。
- fresh host（page cache を落とした直後）は cold と同程度（hello p50 6158 vs 5817）。kernel 17 MiB と rootfs 64 MiB の読み込みは boot 全体に比べて小さい。gateway の起動は fresh host で p50 500 / 最大 2080 ms、deploy（CLI）は p50 277 ms。
- warm は同じ環境を再利用し、client p50 は hello 56 ms。基盤の足し分（host platform）は p50 6〜8 ms、p95 17〜31 ms。

### 6.2 同時実行（総 24 request、失敗を含む client p50 / p95）

| sample | pool | 並列 1 | 並列 2 | 並列 4 | 並列 8 |
|---|---|---|---|---|---|
| hello | off | 5838 / 6475（0.17 件/s） | 6381 / 7356（0.30） | 11825 / 27970（0.29） | 10126 / 19429（0.42、**504 × 4**） |
| hello | on | 42 / 73（14.75） | 45 / 78（3.33、cold 1） | 50 / 6388（3.55、cold 2） | 142 / 6439（3.53、cold 2） |
| http-axum | off | 5926 / 8581（0.16） | 6157 / 8354（0.31） | 7730 / 8966（0.50） | 14611 / 22369（0.42、**504 × 2**） |
| http-axum | on | 390 / 887（1.85） | 127 / 855（1.13、cold 1） | 92 / 9839（2.30、cold 2） | 167 / 30110（0.70、**502 × 2**） |
| cpu-burn | off | 6053 / 12967（0.13） | 6123 / 6820（0.32） | 7575 / 15052（0.44） | 14186 / 23350（0.41、**504 × 3**） |
| cpu-burn | on | 331 / 434（1.22、cold 1） | 279 / 295（2.54、cold 1） | 282 / 5954（3.20、cold 2） | 548 / 747（3.60、cold 1） |

- 429 と 503 は 0。pool 無効の並列 8 では revision の `max_concurrency = 4` を超えた分が queue に入り（queue p95 9179 ms）、`queue_timeout_seconds = 10` を超えた request が 504 `Host.QueueTimeout` になった。cold の起動が 5 秒以上かかるため、4 本の枠が 2 巡する前に queue deadline が来る。
- http-axum の pool 有効・並列 8 の 502 × 2 は `Host.EnvironmentBootFailed`（30 秒の bridge 待ちで timeout。pool に足りない分を同時に cold 起動した）。
- pool 有効では、並列度が pool 内の環境数を超えた瞬間だけ cold が混じり、その request が p95 を 6〜10 秒に押し上げる。成功数/秒は pool 無効の 1.7〜87 倍（並列 1 で最大、http-axum の並列 8 で最小）。
- 並列度を上げても pool 無効のスループットは 0.4〜0.5 件/s で頭打ち（4 vCPU の VM で 500 m の VM を 4〜8 台同時に boot する）。

### 6.3 資源原価（環境 1 つあたり、host から見た値）

| 項目 | 値 |
|---|---|
| 要求 memory / cgroup `memory.max` | 256 MiB / 320 MiB（要求 + 64 MiB） |
| 休止中の VMM RSS | 38.6〜38.8 MiB（60 秒間変化なし） |
| 休止中の cgroup `memory.current`（anon / file） | 40.4〜41.2 MiB（anon 36.5 / file 3.3） |
| 寿命全体の `memory.peak`（370 環境） | p50 41.0 / p95 41.6 / 最大 43.4 MiB。OOM kill 0 |
| 休止 60 秒の CPU | VMM の tick 増分 0、cgroup `usage_usec` 増分 0（3 sample とも）。gateway は 60 秒で 3 tick（30 ms） |
| 寿命全体の CPU（cold の 1 回分を含む） | hello p50 3.2 s / p95 6.3 s（うち throttled p50 3.2 s） |
| gateway（host 側 bridge session） | 環境 0 で RSS 19.5 MiB、pool に 1 つで 22.3 MiB、sweep 後に 4〜6 つで 34〜39 MiB（1 環境あたり概ね 2.5〜3 MiB。sweep 中の確保も含む上限の見積もり） |
| host disk（env dir + jail） | 349.6 MiB。うち scratch drive 256 MiB と function drive 10 MiB が環境ごと、rootfs 64 MiB と kernel 17 MiB は `.kvm` からの hard link（全環境で共有）なので、環境ごとの増分は約 268 MiB |
| 転送量 | client ↔ gateway の body は hello 16 → 84 bytes、http-axum 0 → 2 bytes、cpu-burn 15 → 70 bytes（header 約 260 bytes）。guest の network は egress none で 0 |
| guest 側 bridge | rootfs 内の static バイナリ 4.96 MiB（guest 内の RSS は未測定） |

休止は memory を返さない。要求 256 MiB に対し host が実際に保持するのは 1 環境あたり約 41 MiB（guest が触った分）で、4 vCPU / 8 GiB の VM で pool に 6 環境を置いても host の MemAvailable は約 244 MiB（7356.6 → 7112.2 MiB、hello）しか減らなかった。

### 6.4 RFC 仮目標（RFC §19）との比較

| 目標 | 判定 | 測定 | 理由 |
|---|---|---|---|
| cold first response（小さな Rust 関数、image cache hit）p95 ≤ 3 秒 | **未達** | hello cold p95 6037 ms（n=20、失敗 0） | boot p95 5315 ms のうち大半が guest kernel の起動で、500 m の cgroup quota に throttle され続けている（throttled period 比 0.99）。nested virtualization の serial console が重い。bare metal・console 抑制・quota 変更時の値は未測定なので、構造的に 3 秒を超えるかは判断できない |
| cache miss を別集計 | 記録のみ | hello 6158 / 7609（n=5）、http-axum 5908 / 6128、cpu-burn 6925 / 14106 | RFC に数値目標は無い |
| warm の基盤追加遅延 p95 ≤ 20 ms（handler 除く） | **一部未達**（hello 17 ms・cpu-burn 17 ms は達成、http-axum 31 ms は未達） | host 計測の total − handler。client から見た client − handler は 23 / 51 / 30 ms | 逐次・loopback・1 host の値。http-axum の p95 は readiness（p95 15 ms）を含む。内訳ごとの揺れの原因は調べていない。RFC の「同一 region・所定負荷」の条件ではない |
| Fast Restore が cold より p95 / p99 と原価で改善 | 未測定 | — | snapshot restore が未実装（§8） |
| 正常 invoke の可用性 99.9% | 未測定 | 失敗率は逐次の group で 0、並列 8 で 8〜17% | 数百 request・1 host の記録は可用性の測定ではない |

### 6.5 P0 の Kata / Knative 条件との比較

P0（PLT-4613 / PLT-4616）では Kata・Cloud Hypervisor・Knative のどれも実行していないので、**数値の比較はできない**（`summary.md` §5）。設定値として分かっているのは tachyon-apps の RuntimeClass `kata` の `overhead.podFixed` = cpu 250m / memory 256Mi（宣言値）と ResourceQuota pods 20 だけで、本測定の「1 環境あたり host が保持する約 41 MiB、休止中 CPU 0」と同じ尺度の値は Kata 側に無い。運用構成の比較（host 1 台 + gateway 1 プロセス vs k3s + kata-deploy + RuntimeClass (+ Knative Serving)）は `summary.md` §5 の表のとおり。

## 7. 注意

- **nested virtualization**: Apple M4 → macOS の Virtualization.framework → Lima VM（Linux aarch64、4 vCPU / 8 GiB）→ KVM → Firecracker。guest の MMIO（とくに serial console）と timer の exit が二重になるので、bare metal より桁で遅くなりうる。x86_64 と bare metal の値は未測定。
- **serial console**: provider は証跡のため `console=ttyS0` で kernel ログを出す（`docs/kvm.md` §6）。この host では cold の起動中ずっと 500 m の cgroup quota に当たって throttle されている（summary の §3.2 の throttled period 比 0.99）。そのうち console 出力がどれだけを占めるかは測っていない。`quiet` や quota の変更は provider の挙動を変えるので本計測ではしていない。
- **物理 host の共用**: 計測に使った Mac は他の build と共用で、負荷が高い間（load average 40〜140）に始めた 1 回目の実行（`bench-20260917T050042Z`、hello だけで中止）では、hello の fresh host 5 回中 4 回、cold 20 回中 5 回が失敗した（30 秒の bridge 待ち timeout の `Host.EnvironmentBootFailed` / `Host.InitTimeout`、および `firecracker exited before creating the API socket`（console / fc.log とも空）の起動失敗 2 件）。成功した cold の boot も 15〜27 秒だった。この実行は物理 host の混雑を測ってしまうため途中で止め、生データは残していない（**除外した実行があることをここに記録する**）。API socket 前の終了の原因は調べていない。
- **1 host・同居**: client（curl）、gateway、全 VMM が同じ 4 vCPU を使う。並列度 8 では client と gateway も CPU を取り合う。
- **固定負荷**: sweep の総 request 数は固定で、到着率を制御した負荷（open loop）ではない。スループットは「この総数を並列 n で流したときの成功数 / 壁時計」。
- **cache miss の範囲**: `drop_caches` は host の page cache を落とすが、macOS 側（Lima の disk image の下）の cache は落とせない。本当の「新品の専用 host」ではない。
- **SLA・価格ではない**: 失敗率・可用性・原価は数百 request の 1 回の記録で、提供値や単価の根拠にしない（RFC §16.3、§19）。

## 8. Fast Restore（X1）を追加するとき

snapshot restore（PLT-4653 以降）が provider に入ったら、`bench.sh` に `restore-hit` / `restore-miss` の scenario を足す。手順は `warm` と同じ構造にし、同じ `request` / `enrich` / `bench-report.sh` を通して client の p50 / p95 / p99、失敗率、`AttemptTimings` の内訳（restore 用の timing が増える場合はそれも）、restore 後の環境の memory.current を並べる。RFC §13.5・§19 のとおり、restore API の完了時間ではなく **最初の応答まで**を比べ、cold と同じ表に置く。snapshot が使えず通常起動に落ちた attempt は除外せず、start kind で数える。

## 9. 未測定・未検証

| 項目 | 状態 | 理由 |
|---|---|---|
| x86_64 host、bare metal、専用の新品 host | 未検証 | 記録は Lima VM の nested virtualization だけ |
| Kata / Cloud Hypervisor / Knative との数値比較 | 未測定 | P0 ではどれも実行していない（`summary.md` §5）。Kata adapter も Knative 構成も本リポジトリに無い |
| guest 内の memory 内訳（kernel / bridge / 関数） | 未測定 | host から見えない。VMM RSS と cgroup の値だけ |
| vsock の転送 byte 数、1 MiB payload / 6 MiB response（ADR-0001 M13） | 未測定 | 本計測の payload は数十 bytes |
| open loop の到着率負荷、長時間の soak、memory pressure、noisy neighbor 下の first response | 未測定 | noisy neighbor の CPU 隔離は `docs/kvm.md` §3.6 NOISY で別に測っている |
| Fast Restore | 未測定 | 実装なし（§8） |
