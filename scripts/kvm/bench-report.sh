#!/usr/bin/env bash
# scripts/kvm/bench-report.sh - turn the raw files of a scripts/kvm/bench.sh run into summary.json
# and summary.md (PLT-4647). Reads only the evidence directory, so it can be rerun offline (for
# example on a laptop after the evidence was copied back).
#
# Usage: scripts/kvm/bench-report.sh docs/evidence/bench-<UTC>
#
# Rules (docs/benchmark.md):
#   - percentiles are nearest-rank over every attempt of the group, failures included; the failure
#     rate is reported next to them and nothing is dropped;
#   - a timing percentile is over the attempts that carry that timing, and its n is printed;
#   - the RFC targets are compared as 達成 / 未達 / 未測定 with the reason; nothing here is an SLA or
#     a price.
set -euo pipefail

[ $# -eq 1 ] || { echo "usage: $0 <evidence-dir>" >&2; exit 2; }
DIR="$1"
[ -s "$DIR/metadata.json" ] || { echo "$DIR/metadata.json missing" >&2; exit 2; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }

slurp() { if [ -s "$1" ]; then jq -s . "$1"; else echo '[]'; fi; }

# Teardown cgroup stats the provider logs for every environment (all gateway runs).
TEARDOWN="$(cat "$DIR"/gateway-logs/*.log 2>/dev/null |
  jq -Rc 'fromjson? | select(.message == "cgroup stats at teardown") | {env: .env_id, ts: .timestamp, stats: (.stats | fromjson? // null)}' |
  jq -s . )"
[ -n "$TEARDOWN" ] || TEARDOWN='[]'

jq -n \
  --argjson meta "$(cat "$DIR/metadata.json")" \
  --argjson rows "$(slurp "$DIR/attempts.jsonl")" \
  --argjson sweeps "$(slurp "$DIR/sweeps.jsonl")" \
  --argjson res "$(slurp "$DIR/resources.jsonl")" \
  --argjson gw "$(slurp "$DIR/gateway-runs.jsonl")" \
  --argjson deploys "$(slurp "$DIR/deploys.jsonl")" \
  --argjson teardown "$TEARDOWN" '
  def pct($p): sort as $s | ($s | length) as $n
    | if $n == 0 then null else $s[([((($p / 100) * $n) | ceil) - 1, 0] | max)] end;
  def dist: map(select(. != null)) as $v
    | {n: ($v | length), p50: ($v | pct(50)), p95: ($v | pct(95)), p99: ($v | pct(99)),
       min: ($v | min), max: ($v | max),
       mean: (if ($v | length) > 0 then (($v | add) / ($v | length) | . * 10 | round / 10) else null end)};
  def ok: .http_code == 200 and .status == "succeeded";
  def t($k): (.timings // {})[$k];
  def derive: . + {
      ok: ok,
      platform_ms: (if t("total_ms") != null and t("handler_ms") != null then t("total_ms") - t("handler_ms") else null end),
      client_platform_ms: (if .client_ms != null and t("handler_ms") != null then .client_ms - t("handler_ms") else null end),
      client_extra_ms: (if .client_ms != null and t("total_ms") != null then .client_ms - t("total_ms") else null end),
      unaccounted_ms: (if t("total_ms") != null then
          t("total_ms") - ([t("queue_wait_ms"), t("environment_boot_ms"), t("runtime_init_ms"), t("resume_ms"),
                            t("readiness_ms"), t("handler_ms"), t("response_ms")] | map(. // 0) | add)
        else null end)};
  def group_stats: . as $g | {
      n: ($g | length),
      succeeded: ([$g[] | select(.ok)] | length),
      failed: ([$g[] | select(.ok | not)] | length),
      failure_rate: (if ($g | length) > 0 then (([$g[] | select(.ok | not)] | length) / ($g | length) * 1000 | round / 1000) else null end),
      http_codes: ($g | group_by(.http_code) | map({key: (.[0].http_code | tostring), value: length}) | from_entries),
      error_types: ([$g[] | select(.ok | not) | (.error_type // .invocation_error.error_type // .error_code // .curl_error // "unknown")]
                    | group_by(.) | map({key: .[0], value: length}) | from_entries),
      start_kinds: ($g | group_by(.start_kind // "none") | map({key: (.[0].start_kind // "none"), value: length}) | from_entries),
      retried_attempts: ([$g[] | select(.attempts > 1)] | length),
      client_ms: ([$g[].client_ms] | dist),
      client_ttfb_ms: ([$g[].client_ttfb_ms] | dist),
      total_ms: ([$g[] | t("total_ms")] | dist),
      queue_wait_ms: ([$g[] | t("queue_wait_ms")] | dist),
      environment_boot_ms: ([$g[] | select(.start_kind == "cold") | t("environment_boot_ms")] | dist),
      runtime_init_ms: ([$g[] | select(.start_kind == "cold") | t("runtime_init_ms")] | dist),
      resume_ms: ([$g[] | t("resume_ms")] | dist),
      readiness_ms: ([$g[] | t("readiness_ms")] | dist),
      handler_ms: ([$g[] | t("handler_ms")] | dist),
      response_ms: ([$g[] | t("response_ms")] | dist),
      platform_ms: ([$g[].platform_ms] | dist),
      client_platform_ms: ([$g[].client_platform_ms] | dist),
      client_extra_ms: ([$g[].client_extra_ms] | dist),
      unaccounted_ms: ([$g[].unaccounted_ms] | dist),
      function_drive_bytes: ([$g[].env.function_drive_bytes] | dist),
      scratch_drive_ms: ([$g[] | select(.start_kind == "cold") | .env.scratch_drive_ms] | dist),
      bytes_up_body: ([$g[].bytes_up_body] | dist),
      bytes_down_body: ([$g[].bytes_down_body] | dist),
      bytes_down_headers: ([$g[].bytes_down_headers] | dist)};

  ($rows | map(derive)) as $r
  | ($meta.load.samples) as $samples
  | {
      run: $meta.run, commit: $meta.commit,
      rules: "nearest-rank percentiles over all attempts of a group including failures; timing percentiles over attempts carrying the timing (n shown); nothing dropped",
      scenarios: [ $samples[] as $s | ["fresh-miss", "cold", "warm-prime", "warm"][] as $sc
        | [$r[] | select(.sample == $s and .scenario == $sc)] as $g
        | select(($g | length) > 0)
        | {sample: $s, scenario: $sc} + ($g | group_stats)
        | . + {warm_only: (if $sc == "warm" then ([$g[] | select(.start_kind == "warm")] | group_stats | {n, client_ms, platform_ms, client_platform_ms}) else null end)} ],
      sweeps: [ $sweeps[] as $w
        | [$r[] | select(.sample == $w.sample and .scenario == ("sweep-" + $w.mode) and .concurrency == $w.concurrency)] as $g
        | {sample: $w.sample, mode: $w.mode, concurrency: $w.concurrency, requests: $w.requests, wall_ms: $w.wall_ms,
           throughput_ok_per_s: (if $w.wall_ms > 0 then (([$g[] | select(.ok)] | length) / ($w.wall_ms / 1000) * 100 | round / 100) else null end),
           status_429: ([$g[] | select(.http_code == 429)] | length),
           status_503: ([$g[] | select(.http_code == 503)] | length),
           status_504: ([$g[] | select(.http_code == 504)] | length)}
          + ($g | group_stats | {n, succeeded, failed, failure_rate, http_codes, error_types, start_kinds, client_ms, queue_wait_ms, total_ms, handler_ms, platform_ms}) ],
      gateway_starts: {
        fresh_host: ([$gw[] | select(.label | test("-fresh-")) | .start_to_ready_ms] | dist),
        warm_config_page_cache_warm: ([$gw[] | select(.kind == "warm") | .start_to_ready_ms] | dist)},
      deploys: {create_client_ms: ([$deploys[].create_client_ms] | dist), deploy_client_ms: ([$deploys[].deploy_client_ms] | dist)},
      resources: {
        paused_idle: [ $samples[] as $s
          | ([$res[] | select(.sample == $s and .phase == "idle-start")][0]) as $a
          | ([$res[] | select(.sample == $s and .phase == "idle-end")][0]) as $b
          | select($a != null and $b != null)
          | {sample: $s, window_s: (($b.ts_ms - $a.ts_ms) / 1000 | round), clk_tck: $a.host.clk_tck,
             environments: [ $a.environments[] as $e
               | ([$b.environments[] | select(.env == $e.env)][0]) as $f
               | select($f != null)
               | ($e.processes | map(select(.comm == "firecracker"))[0]) as $p0
               | ($f.processes | map(select(.comm == "firecracker"))[0]) as $p1
               | {env: $e.env,
                  requested_memory_mib: $meta.profile.memory_mib,
                  vmm_state: ($p1.state // null),
                  vmm_rss_kib_start: ($p0.vm_rss_kib // null), vmm_rss_kib_end: ($p1.vm_rss_kib // null),
                  vmm_hwm_kib: ($p1.vm_hwm_kib // null),
                  vmm_cpu_ticks_delta: (if $p0 != null and $p1 != null then $p1.cpu_ticks - $p0.cpu_ticks else null end),
                  cgroup_memory_current_bytes_start: $e.cgroup.memory_current_bytes,
                  cgroup_memory_current_bytes_end: $f.cgroup.memory_current_bytes,
                  cgroup_memory_peak_bytes: $f.cgroup.memory_peak_bytes,
                  cgroup_memory_max: $f.cgroup.memory_max,
                  cgroup_memory_stat_end: $f.cgroup.memory_stat,
                  cgroup_cpu_usage_usec_delta: (if $e.cgroup.cpu_usage_usec != null and $f.cgroup.cpu_usage_usec != null then $f.cgroup.cpu_usage_usec - $e.cgroup.cpu_usage_usec else null end),
                  env_dir_bytes: $f.disk.env_dir_bytes, jail_dir_bytes: $f.disk.jail_dir_bytes,
                  env_and_jail_unique_bytes: $f.disk.env_and_jail_unique_bytes}],
             gateway_rss_kib_start: ($a.gateway.vm_rss_kib // null), gateway_rss_kib_end: ($b.gateway.vm_rss_kib // null),
             gateway_cpu_ticks_delta: (if $a.gateway != null and $b.gateway != null then $b.gateway.cpu_ticks - $a.gateway.cpu_ticks else null end),
             host_mem_available_kib_start: $a.host.mem_available_kib, host_mem_available_kib_end: $b.host.mem_available_kib} ],
        gateway_rss_by_pooled_environments: [ $res[] | select(.gateway != null)
          | {sample, phase, pooled_environments: (.environments | length), gateway_rss_kib: .gateway.vm_rss_kib,
             host_mem_available_kib: .host.mem_available_kib} ],
        pooled_after_sweep: [ $res[] | select(.phase == "warm-after-sweep")
          | {sample, environments: (.environments | length),
             cgroup_memory_current_bytes: ([.environments[].cgroup.memory_current_bytes] | dist),
             vmm_rss_kib: ([.environments[].processes[] | select(.comm == "firecracker") | .vm_rss_kib] | dist),
             env_and_jail_unique_bytes: ([.environments[].disk.env_and_jail_unique_bytes] | dist)} ],
        teardown_by_sample: [ $samples[] as $s
          | ([$r[] | select(.sample == $s) | .environment_id] | unique) as $envs
          | [$teardown[] | select(.env as $e | $envs | index($e))] as $td
          | {sample: $s, environments: ($td | length),
             memory_peak_bytes: ([$td[].stats["memory.peak"]] | dist),
             cpu_usage_usec: ([$td[].stats["cpu.stat"].usage_usec] | dist),
             cpu_throttled_usec: ([$td[].stats["cpu.stat"].throttled_usec] | dist),
             throttled_period_ratio: ([$td[] | .stats["cpu.stat"] | select(.nr_periods > 0) | (.nr_throttled / .nr_periods * 100 | round / 100)] | dist),
             oom_kills: ([$td[].stats["memory.events"].oom_kill // 0] | add // 0)} ]}
    }
  | . as $sum
  | def sc($s; $n): [$sum.scenarios[] | select(.sample == $s and .scenario == $n)][0];
    . + {rfc_targets: [
      (sc("hello"; "cold")) as $c
      | {target: "cold first response: small Rust function, image cache hit, p95 <= 3 s (RFC §19)",
         measured: (if $c == null then null else {sample: "hello", n: $c.n, failure_rate: $c.failure_rate, client_p95_ms: $c.client_ms.p95, client_p99_ms: $c.client_ms.p99} end),
         verdict: (if $c == null then "未測定" elif $c.failed > 0 then "未達" elif $c.client_ms.p95 <= 3000 then "達成" else "未達" end),
         reason: (if $c == null then "hello cold was not run"
                  elif $c.failed > 0 then "failed attempts in the group count as misses"
                  elif $c.client_ms.p95 <= 3000 then "p95 within 3000 ms on this host (aarch64 nested virtualization, not the baseline profile)"
                  else "p95 above 3000 ms on this host; see environment_boot_ms and the cgroup throttling ratio for where the time goes" end)},
      ([$sum.scenarios[] | select(.scenario == "fresh-miss")] | map({sample, n, client_p95_ms: .client_ms.p95, client_max_ms: .client_ms.max, failure_rate})) as $m
      | {target: "cold first response, image cache miss: reported separately (RFC §19)",
         measured: $m, verdict: (if ($m | length) == 0 then "未測定" else "記録のみ（目標値なし）" end),
         reason: "RFC gives no number for the miss case; recorded so it is not mixed into the hit percentiles"},
      ([$sum.scenarios[] | select(.scenario == "warm")] | map({sample, n, start_kinds, failure_rate,
          host_platform_p95_ms: .platform_ms.p95, client_platform_p95_ms: .client_platform_ms.p95})) as $w
      | {target: "warm platform added latency p95 <= 20 ms, user handler excluded (RFC §19)",
         measured: $w,
         verdict: (if ($w | length) == 0 then "未測定"
                   elif ([$w[] | select(.failure_rate > 0 or .host_platform_p95_ms == null or .host_platform_p95_ms > 20)] | length) == 0 then "達成（host 計測）"
                   else "未達" end),
         reason: "host_platform = total_ms - handler_ms (queue, resume, readiness, response, bookkeeping) over every attempt of the warm group including any that fell back to cold; client_platform adds loopback HTTP and curl. RFC asks for same-region under a set load; this is loopback on one nested host, sequential"},
      {target: "Fast Restore: end-to-end p95/p99 and cost better than cold (RFC §19)", measured: null, verdict: "未測定",
       reason: "snapshot restore is not implemented (Capabilities.snapshot_clone = unsupported); X1 adds it to this benchmark"},
      {target: "normal invoke availability 99.9% (RFC §19)", measured: ([$sum.scenarios[] | {sample, scenario, n, failure_rate}]), verdict: "未測定",
       reason: "a few hundred requests on one host are not an availability measurement; failure rates are listed for the record only"}
    ]}
' > "$DIR/summary.json"

# ---------------------------------------------------------------------------
# summary.md
# ---------------------------------------------------------------------------
jq -r --argjson meta "$(cat "$DIR/metadata.json")" '
  def v: if . == null then "-" else tostring end;
  def d3($x): "\($x.p50 | v) / \($x.p95 | v) / \($x.p99 | v)";
  def mib: if . == null then "-" else (. / 1048576 * 10 | round / 10 | tostring) end;
  def kib2mib: if . == null then "-" else (. / 1024 * 10 | round / 10 | tostring) end;
  def kv: to_entries | map("\(.key)=\(.value)") | join(" ");
  "# Benchmark \(.run)（PLT-4647）",
  "",
  "**nested virtualization 上の aarch64 の記録であり、bare metal の x86_64 を代表しない。SLA・販売価格の根拠にしない。** 読み方と再実行は `docs/benchmark.md`。",
  "",
  "## 条件",
  "",
  "| 項目 | 値 |", "|---|---|",
  "| commit | `\($meta.commit)`（worktree dirty: \($meta.worktree_dirty)） |",
  "| host | \($meta.host.uname) |",
  "| CPU / memory | \($meta.host.cpus) vCPU（vendor \($meta.host.cpu_vendor // "-")、model \($meta.host.cpu_model // "unknown")）/ MemTotal \($meta.host.mem_total_kib) KiB |",
  "| physical host | \($meta.host.physical_host // "not recorded") |",
  "| KVM / nested | \($meta.host.kvm) / nested virtualization = \($meta.host.nested_virtualization)（virtualization: \($meta.host.virtualization // "-")） |",
  "| Firecracker / jailer | \($meta.firecracker.version) / \($meta.jailer_version) |",
  "| guest kernel sha256 | `\($meta.guest_kernel.sha256)`（\($meta.guest_kernel.key // "-")） |",
  "| rootfs sha256 | `\($meta.rootfs.sha256)`（\($meta.rootfs.bytes) bytes、bridge \($meta.runtime_bridge.bytes) bytes） |",
  "| profile | \($meta.profile.isolation)、\($meta.profile.memory_mib) MiB、\($meta.profile.cpu_millis) m、timeout \($meta.profile.timeout_seconds) s、revision max_concurrency \($meta.profile.revision_max_concurrency) |",
  "| gateway | \($meta.gateway.build_profile) build、\($meta.gateway.runs_as)、config `gateway-cold.toml` / `gateway-warm.toml`（`[pool] enabled = true`） |",
  "| 負荷 | fresh host \($meta.load.fresh_host_trials_per_sample) 回 / cold \($meta.load.cold_n) 回 / warm \($meta.load.warm_n) 回（間隔 \($meta.load.warm_gap_ms) ms）/ sweep \($meta.load.sweep_levels | map(tostring) | join(",")) 並列 × 各 \($meta.load.sweep_requests_per_level) request（\($meta.load.sweep_modes | join(" / "))）/ idle \($meta.load.idle_seconds) s |",
  "| payload | \($meta.load.payloads | kv) |",
  "| client / seed | \($meta.load.client)。\($meta.load.seeds) |",
  "",
  "percentile は nearest-rank。client の分布は **失敗を含む全 attempt**、内訳の分布はその値を持つ attempt（n を併記）。外れ値は除外していない。生データは `attempts.jsonl`（1 request 1 行）と `invocations.jsonl`。",
  "",
  "## 1. first response（ms、p50 / p95 / p99）",
  "",
  "| sample | scenario | n | 失敗率 | start kind | client | queue | boot | init | resume | readiness | handler | host platform（total−handler） | client−total |",
  "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
  (.scenarios[] | "| \(.sample) | \(.scenario) | \(.n) | \(.failure_rate) | \(.start_kinds | kv) | \(d3(.client_ms)) | \(d3(.queue_wait_ms)) | \(d3(.environment_boot_ms)) | \(d3(.runtime_init_ms)) | \(d3(.resume_ms)) | \(d3(.readiness_ms)) | \(d3(.handler_ms)) | \(d3(.platform_ms)) | \(d3(.client_extra_ms)) |"),
  "",
  "- `fresh-miss`: gateway 停止 → data_dir と workdir を削除 → `drop_caches` → gateway 起動 → create / deploy → 最初の invoke（kernel・rootfs・VMM・artifact が page cache に無い）。n は fresh host の試行数で、20 未満のため p95 / p99 は最大値に近い。",
  "- `cold`: 同じ gateway、pool 無効。page cache は温まっているが **function drive と scratch drive は環境ごとに毎回作る**（drive の cache は実装に無い）。",
  "- `warm`: pool 有効の gateway で 1 回 prime した後の連続 invoke。warm にならなかった attempt も除外せず含める（start kind の列）。",
  "- boot / init は cold attempt だけ、resume / readiness は warm attempt だけが持つ。",
  (if ([.scenarios[] | select(.retried_attempts > 0)] | length) > 0 then "- 2 attempt になった invocation（warm dispatch 失敗 → cold 再試行）: " + ([.scenarios[] | select(.retried_attempts > 0) | "\(.sample)/\(.scenario) \(.retried_attempts)"] | join(", ")) else "- 2 attempt になった invocation は無い。" end),
  (if ([.scenarios[] | select(.failed > 0)] | length) > 0 then "- 失敗: " + ([.scenarios[] | select(.failed > 0) | "\(.sample)/\(.scenario) \(.failed) 件（\(.error_types | kv)）"] | join("; ")) else "- 失敗した attempt は無い。" end),
  "",
  "warm のうち start kind が warm の attempt だけ（参考）:",
  "",
  "| sample | n | client | host platform | client platform（client−handler） |", "|---|---|---|---|---|",
  (.scenarios[] | select(.scenario == "warm") | "| \(.sample) | \(.warm_only.n) | \(d3(.warm_only.client_ms)) | \(d3(.warm_only.platform_ms)) | \(d3(.warm_only.client_platform_ms)) |"),
  "",
  "gateway の起動（start → readyz 200）: fresh host \(d3(.gateway_starts.fresh_host))（n=\(.gateway_starts.fresh_host.n)）、page cache が温まった状態 \(d3(.gateway_starts.warm_config_page_cache_warm))（n=\(.gateway_starts.warm_config_page_cache_warm.n)）。deploy（CLI、upload + revision ready）: \(d3(.deploys.deploy_client_ms))（n=\(.deploys.deploy_client_ms.n)）。",
  "",
  "## 2. 同時実行（sweep）",
  "",
  "| sample | pool | 並列 | n | 成功 | 失敗率 | 429 | 503 | 504 | start kind | 成功/秒 | client p50 / p95 / p99 | queue p50 / p95 / p99 | host platform p50 / p95 / p99 |",
  "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
  (.sweeps[] | "| \(.sample) | \(if .mode == "warm" then "on" else "off" end) | \(.concurrency) | \(.n) | \(.succeeded) | \(.failure_rate) | \(.status_429) | \(.status_503) | \(.status_504) | \(.start_kinds | kv) | \(.throughput_ok_per_s) | \(d3(.client_ms)) | \(d3(.queue_wait_ms)) | \(d3(.platform_ms)) |"),
  "",
  "上限: gateway `[capacity] max_concurrency = 8`、`max_queue = 32`、`queue_timeout_seconds = 10`、revision の max_concurrency は上表の条件。総 request 数は並列度によらず固定（VM を飽和させないため）。",
  (if ([.sweeps[] | select(.failed > 0)] | length) > 0 then "失敗の内訳: " + ([.sweeps[] | select(.failed > 0) | "\(.sample) pool=\(.mode) c=\(.concurrency): \(.error_types | kv)"] | join("; ")) else "sweep の失敗は無い。" end),
  "",
  "## 3. 資源原価",
  "",
  "### 3.1 休止中（pool）の環境 1 つあたり",
  "",
  "| sample | 窓 s | env | 要求 memory MiB | VMM state | VMM RSS MiB（始 → 終） | cgroup memory.current MiB（始 → 終） | memory.peak MiB | anon / file MiB | VMM CPU tick 増分 | cgroup CPU usec 増分 | host disk（env dir + jail、重複除く）MiB |",
  "|---|---|---|---|---|---|---|---|---|---|---|---|",
  (.resources.paused_idle[] as $p | $p.environments[] | "| \($p.sample) | \($p.window_s) | `\(.env)` | \(.requested_memory_mib) | \(.vmm_state | v) | \(.vmm_rss_kib_start | kib2mib) → \(.vmm_rss_kib_end | kib2mib) | \(.cgroup_memory_current_bytes_start | mib) → \(.cgroup_memory_current_bytes_end | mib) | \(.cgroup_memory_peak_bytes | mib) | \(.cgroup_memory_stat_end.anon | mib) / \(.cgroup_memory_stat_end.file | mib) | \(.vmm_cpu_ticks_delta | v) | \(.cgroup_cpu_usage_usec_delta | v) | \(.env_and_jail_unique_bytes | mib) |"),
  "",
  (.resources.paused_idle[] | "- \(.sample): gateway RSS \(.gateway_rss_kib_start | kib2mib) → \(.gateway_rss_kib_end | kib2mib) MiB、gateway CPU tick 増分 \(.gateway_cpu_ticks_delta | v)（CLK_TCK \(.clk_tck)）、host MemAvailable \(.host_mem_available_kib_start | kib2mib) → \(.host_mem_available_kib_end | kib2mib) MiB"),
  "",
  "### 3.2 環境の寿命全体（provider が teardown 時に記録する cgroup の値。cold と warm の全環境）",
  "",
  "| sample | 環境数 | memory.peak MiB p50 / p95 / max | CPU usage ms p50 / p95 / max | throttled ms p50 / p95 | throttled period 比 p50 / p95 | OOM kill |",
  "|---|---|---|---|---|---|---|",
  (.resources.teardown_by_sample[] | "| \(.sample) | \(.environments) | \(.memory_peak_bytes.p50 | mib) / \(.memory_peak_bytes.p95 | mib) / \(.memory_peak_bytes.max | mib) | \(if .cpu_usage_usec.p50 == null then "-" else (.cpu_usage_usec.p50 / 1000 | round | tostring) end) / \(if .cpu_usage_usec.p95 == null then "-" else (.cpu_usage_usec.p95 / 1000 | round | tostring) end) / \(if .cpu_usage_usec.max == null then "-" else (.cpu_usage_usec.max / 1000 | round | tostring) end) | \(if .cpu_throttled_usec.p50 == null then "-" else (.cpu_throttled_usec.p50 / 1000 | round | tostring) end) / \(if .cpu_throttled_usec.p95 == null then "-" else (.cpu_throttled_usec.p95 / 1000 | round | tostring) end) | \(.throttled_period_ratio.p50 | v) / \(.throttled_period_ratio.p95 | v) | \(.oom_kills) |"),
  "",
  "### 3.3 gateway（host 側 bridge session）と pool の規模",
  "",
  "| sample | 時点 | pool 内の環境 | gateway RSS MiB | host MemAvailable MiB |", "|---|---|---|---|---|",
  (.resources.gateway_rss_by_pooled_environments[] | "| \(.sample) | \(.phase) | \(.pooled_environments) | \(.gateway_rss_kib | kib2mib) | \(.host_mem_available_kib | kib2mib) |"),
  "",
  (.resources.pooled_after_sweep[] | "- \(.sample) sweep 後に pool に残った環境 \(.environments)（上限 max_idle_per_revision = 8）: cgroup memory.current p50 \(.cgroup_memory_current_bytes.p50 | mib) MiB / max \(.cgroup_memory_current_bytes.max | mib) MiB、VMM RSS p50 \(.vmm_rss_kib.p50 | kib2mib) MiB、disk p50 \(.env_and_jail_unique_bytes.p50 | mib) MiB"),
  "",
  "### 3.4 disk と転送量",
  "",
  "| sample | scenario | function drive bytes p50 | scratch drive 作成 ms p50 / p95（cold） | request body bytes p50 | response body bytes p50 | response header bytes p50 |",
  "|---|---|---|---|---|---|---|",
  (.scenarios[] | select(.scenario == "cold" or .scenario == "warm") | "| \(.sample) | \(.scenario) | \(.function_drive_bytes.p50 | v) | \(.scratch_drive_ms.p50 | v) / \(.scratch_drive_ms.p95 | v) | \(.bytes_up_body.p50 | v) | \(.bytes_down_body.p50 | v) | \(.bytes_down_headers.p50 | v) |"),
  "",
  "guest の network 転送量: egress none（NIC 0、`env.network_interfaces`）のため 0。client ↔ gateway は loopback の HTTP で、上表の bytes がそのすべて。gateway ↔ guest の vsock の byte 数は計測していない（未測定）。",
  "",
  "## 4. RFC 仮目標との比較",
  "",
  "| 目標 | 判定 | 測定値 | 理由 |", "|---|---|---|---|",
  (.rfc_targets[] | "| \(.target) | \(.verdict) | `\(.measured | tojson)` | \(.reason) |"),
  "",
  "判定はこの host（aarch64、Apple M4 上の Lima vz、nested virtualization、4 vCPU）での値であり、RFC の想定（x86_64 の国内 host、同一 region の負荷）とは条件が違う。達成でも production の性能を示さず、未達でも bare metal で未達とは限らない。",
  "",
  "## 5. P0 の Kata / Knative 条件との比較",
  "",
  "P0（PLT-4613 / PLT-4616、`docs/inventory-tachyon-apps.md` §3.5・§5、ADR-0001）では **Kata / Cloud Hypervisor / Knative のどれも実行・測定していない**。したがって速さの数値比較はできず、ここに Knative / Kata の数値は書かない。比較できるのは運用構成と容量の前提だけである。",
  "",
  "| 観点 | 本測定（Firecracker provider） | P0 の Kata（tachyon-apps の設定、未実行） | Knative（RFC §6.4、未構築） |",
  "|---|---|---|---|",
  "| 構成要素 | KVM host 1 台、gateway 1 プロセス（root、jailer + cgroup v2）、SQLite 台帳 | k3s + containerd + kata-deploy（`kata-clh`）+ RuntimeClass + namespace quota | k3s + Knative Serving（activator / autoscaler / queue-proxy）+ Kata RuntimeClass |",
  "| 環境 1 つの固定 memory | 上の §3.1 / §3.2 の実測（要求 memory + VMM。cgroup memory.max = 要求 + 64 MiB） | RuntimeClass `overhead.podFixed` = cpu 250m / memory 256Mi（宣言値。実測なし） | Kata の overhead + queue-proxy sidecar（未測定） |",
  "| scale to zero / warm | pool（`[pool]`）で休止、TTL で回収。休止中は CPU tick 0 を §3.1 で測定、memory は返さない | Pod を保持するだけでは実行外 CPU は止まらない（RFC §12.4） | activator 経由の scale-to-zero（未測定） |",
  "| 同時実行の上限 | gateway `max_concurrency` / `max_queue` と revision `max_concurrency`、429 / 504 で拒否（§2） | ResourceQuota pods 20 | containerConcurrency と autoscaler（未測定） |",
  "| 起動の失敗・後始末 | provider が VMM・jail・cgroup・tap を回収し、`cleanup.txt` で残留 0 を確認 | Job TTL と shim | Knative controller（未測定） |",
  "",
  "RFC §6.4 の「同じ Kata・同じイメージ・同じ負荷で Knative と比較する」は未実施（Kata adapter も Knative 構成も本リポジトリに無い）。",
  "",
  "## 6. 後始末",
  "",
  "`cleanup.txt` を参照（gateway・firecracker・jailer プロセス、環境 cgroup、jail、tap、nft table、環境ディレクトリが 0 件であること）。"
' "$DIR/summary.json" > "$DIR/summary.md"

echo "wrote $DIR/summary.json and $DIR/summary.md"
