#!/usr/bin/env bash
# scripts/x1/restore-verify-report.sh - tables of a scripts/x1/restore-verify.sh run (PLT-4654).
# Offline and repeatable (jq only): reads attempts.jsonl, resources.jsonl, teardown-stats.jsonl,
# snapshots.jsonl, checks.tsv, calibration.jsonl of an evidence directory and writes summary.json
# and summary.md next to them.
#
# Rules (docs/benchmark.md §5): nearest-rank percentiles (sorted[ceil(p/100*n)-1]); the client
# distribution is over ALL attempts of a scenario, failures included; breakdown distributions use
# the attempts that carry the value and print n. An attempt fails when HTTP != 200, when the start
# kind is not the scenario's (a restored-* scenario that started cold is a failure, never a
# restore), or when the handler's lookup / spot checks / full checksum are wrong.
#
# Usage: scripts/x1/restore-verify-report.sh <evidence-dir>
set -euo pipefail

[ $# -eq 1 ] || { echo "usage: $0 <evidence-dir>" >&2; exit 2; }
DIR="$1"
for f in attempts.jsonl checks.tsv; do
  [ -f "$DIR/$f" ] || { echo "missing $DIR/$f" >&2; exit 2; }
done
opt() { if [ -s "$DIR/$1" ]; then cat "$DIR/$1"; fi; }

jq -n \
  --slurpfile attempts "$DIR/attempts.jsonl" \
  --slurpfile resources <(opt resources.jsonl) \
  --slurpfile teardown <(opt teardown-stats.jsonl) \
  --slurpfile snapshots <(opt snapshots.jsonl) \
  --slurpfile calibration <(opt calibration.jsonl) \
  --rawfile checks "$DIR/checks.tsv" '
  def pct(p): if length == 0 then null else sort | .[((p / 100 * length) | ceil) - 1 | if . < 0 then 0 else . end] end;
  def dist: map(select(. != null)) | {n: length, p50: pct(50), p95: pct(95), p99: pct(99), max: (max // null)};
  def expected_kind: if startswith("cold") then "cold" elif startswith("restored") or . == "prefer-restored" then "restored"
                     else null end;
  def failed: (.scenario | expected_kind) as $k |
    .http_code != 200 or ($k != null and .start_kind != $k) or
    (.output.dataset.lookup.ok != true) or (.output.dataset.spot_ok != true) or
    (.output.dataset.full != null and .output.dataset.full.ok != true);
  def mib: if . == null then null else (. / 1048576 * 10 | round / 10) end;

  ($teardown | map({key: .env, value: .stats}) | from_entries) as $td |
  ($resources | group_by(.env) | map({key: .[0].env, value: {
      rss_kib: (map(.rss_kib) | max), pss_kib: (map(.pss_kib) | max),
      shared_clean_kib: (map(.shared_clean_kib) | max), private_dirty_kib: (map(.private_dirty_kib) | max),
      memory_current_max: (map(.memory_current) | max), maps_snapshot_mem: (map(.maps_snapshot_mem) | any),
      samples: length}}) | from_entries) as $res |
  ["cold-hit", "cold-miss", "warm", "restored-first", "restored-second", "restored-hit", "restored-miss",
   "restored-plain-miss", "restored-concurrent"] as $order |
  ($attempts | map(select(.scenario as $s | $order | index($s))) | group_by(.scenario) |
    map(. as $rows | .[0].scenario as $s | {
      scenario: $s,
      n: length,
      failures: (map(select(failed)) | length),
      failure_rate: ((map(select(failed)) | length) / length * 1000 | round / 1000),
      start_kinds: (group_by(.start_kind) | map({key: (.[0].start_kind // "none"), value: length}) | from_entries),
      client_ms: (map(.client_ms) | dist),
      total_ms: (map(.timings.total_ms) | dist),
      breakdown: {
        queue_wait_ms: (map(.timings.queue_wait_ms) | dist),
        environment_boot_ms: (map(.timings.environment_boot_ms) | dist),
        runtime_init_ms: (map(.timings.runtime_init_ms) | dist),
        resume_ms: (map(.timings.resume_ms) | dist),
        readiness_ms: (map(.timings.readiness_ms) | dist),
        handler_ms: (map(.timings.handler_ms) | dist),
        response_ms: (map(.timings.response_ms) | dist),
        restore_verify_ms: (map(.restore.restore_verify_ms) | dist),
        restore_load_ms: (map(.restore.restore_load_ms) | dist),
        restore_doorbell_ms: (map(.restore.restore_doorbell_ms) | dist),
        restore_reconnect_ms: (map(.restore.restore_reconnect_ms) | dist),
        restore_ready_ms: (map(.restore.restore_ready_ms) | dist),
        scratch_drive_copy_ms: (map(.restore.scratch_drive_copy_ms) | dist),
        guest_bootstrap_ms: (map(select(.start_kind == "cold") | .output.dataset.bootstrap_ms) | dist),
        guest_after_restore_ms: (map(select(.start_kind != "warm") | .output.after_restore_ms) | dist),
        guest_handler_ms: (map(.output.handler_ms) | dist),
        client_minus_total_ms: (map(if .client_ms != null and .timings.total_ms != null then .client_ms - .timings.total_ms else null end) | dist)
      },
      memory: {
        teardown_memory_peak_mib: (map($td[.environment_id // ""]["memory.peak"] | mib) | dist),
        sampled_rss_mib: (map($res[.environment_id // ""].rss_kib | if . == null then null else . / 1024 | round end) | dist),
        sampled_pss_mib: (map($res[.environment_id // ""].pss_kib | if . == null then null else . / 1024 | round end) | dist),
        sampled_shared_clean_mib: (map($res[.environment_id // ""].shared_clean_kib | if . == null then null else . / 1024 | round end) | dist),
        sampled_private_dirty_mib: (map($res[.environment_id // ""].private_dirty_kib | if . == null then null else . / 1024 | round end) | dist),
        maps_snapshot_mem: (map($res[.environment_id // ""].maps_snapshot_mem) | map(select(. == true)) | length)
      },
      guest: {
        dataset_mib: (map(.output.dataset.mib) | unique),
        uptime_at_after_restore_ms: (map(.output.clock.guest_uptime_at_after_restore_ms) | dist)
      }
    }) | sort_by(.scenario as $s | $order | index($s))) as $scen |
  {
    scenarios: $scen,
    snapshots: ($snapshots | map(select(.snapshot != null) | {label, http_code, client_ms, id: .snapshot.id, timings: .snapshot.timings})),
    storage: ($snapshots | map(select(.storage != null) | {label} + .storage)),
    calibration: $calibration,
    checks: ($checks | split("\n") | .[1:] | map(select(length > 0) | split("\t") | {check: .[0], result: .[1], detail: .[2]}))
  }' >"$DIR/summary.json"

jq -r '
  def f: if . == null then "-" else tostring end;
  def d: "\(.p50 | f) / \(.p95 | f) / \(.p99 | f)";
  def d2: "\(.p50 | f) / \(.p95 | f) (n=\(.n))";
  "# restore-verify summary (PLT-4654, X1)",
  "",
  "Generated by scripts/x1/restore-verify-report.sh. Percentiles are nearest-rank over all attempts (failures included); breakdown columns use attempts that carry the value. Times in ms, nested virtualization, reference values only (not an SLA).",
  "",
  "## Checks",
  "",
  "| check | result | detail |", "|---|---|---|",
  (.checks[] | "| \(.check) | \(.result) | \(.detail | gsub("\\|"; "/") | .[0:600]) |"),
  "",
  "## First response (client, through the gateway, handler result verified)",
  "",
  "| scenario | n | failures | start kinds | client p50 / p95 / p99 | host total p50 / p95 / p99 |",
  "|---|---|---|---|---|---|",
  (.scenarios[] | "| \(.scenario) | \(.n) | \(.failures) (\(.failure_rate)) | \(.start_kinds | to_entries | map("\(.key) \(.value)") | join(", ")) | \(.client_ms | d) | \(.total_ms | d) |"),
  "",
  "## Breakdown (p50 / p95)",
  "",
  "| scenario | queue | boot / clone+reconnect | init / after_restore | resume | readiness | verify | load | doorbell | reconnect | ready | guest bootstrap | guest after_restore | handler | client - total |",
  "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
  (.scenarios[] | .breakdown as $b | "| \(.scenario) | \($b.queue_wait_ms | d2) | \($b.environment_boot_ms | d2) | \($b.runtime_init_ms | d2) | \($b.resume_ms | d2) | \($b.readiness_ms | d2) | \($b.restore_verify_ms | d2) | \($b.restore_load_ms | d2) | \($b.restore_doorbell_ms | d2) | \($b.restore_reconnect_ms | d2) | \($b.restore_ready_ms | d2) | \($b.guest_bootstrap_ms | d2) | \($b.guest_after_restore_ms | d2) | \($b.handler_ms | d2) | \($b.client_minus_total_ms | d2) |"),
  "",
  "## Host memory per environment (MiB, p50 / p95)",
  "",
  "teardown = cgroup memory.peak at teardown (whole lifetime). sampled = max over 0.5 s samples of the VMM /proc/<pid>/smaps_rollup while alive (short-lived environments may be missed: n shows how many were sampled). A clone maps the snapshot memory file MAP_PRIVATE: Shared_Clean is page cache shared with other clones, Private_Dirty is its own.",
  "",
  "| scenario | cgroup memory.peak | RSS | PSS | Shared_Clean | Private_Dirty | mapped snapshot.mem |",
  "|---|---|---|---|---|---|---|",
  (.scenarios[] | .memory as $m | "| \(.scenario) | \($m.teardown_memory_peak_mib | d2) | \($m.sampled_rss_mib | d2) | \($m.sampled_pss_mib | d2) | \($m.sampled_shared_clean_mib | d2) | \($m.sampled_private_dirty_mib | d2) | \($m.maps_snapshot_mem) |"),
  "",
  "## Snapshots and storage",
  "",
  "| label | id | create (client ms) | pause / create / copy / seal ms |",
  "|---|---|---|---|",
  (.snapshots[] | "| \(.label) | \(.id | f) | \(.client_ms) | \(.timings.pause_ms | f) / \(.timings.create_ms | f) / \(.timings.copy_ms | f) / \(.timings.seal_ms | f) |"),
  "",
  "| label | sealed bytes (encrypted + manifest) | plaintext apparent bytes | plaintext allocated bytes |",
  "|---|---|---|---|",
  (.storage[] | "| \(.label) | \(.sealed_bytes | f) | \(.plaintext_apparent_bytes | f) | \(.plaintext_allocated_bytes | f) |"),
  "",
  "## Calibration (awk loop ms, in-VM)",
  "",
  (.calibration[] | "- \(.phase): \(.awk_loop_ms | map(tostring) | join(" ")) (VM loadavg \(.vm_loadavg))")
' "$DIR/summary.json" >"$DIR/summary.md"
echo "$DIR/summary.md"
