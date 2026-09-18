#!/bin/bash
# R4 (PLT-4647 / PLT-4649): cold / warm first response and the parallel sweep
. "$(dirname "$0")/common.sh"
W=$RUNS/bench; sudo rm -rf "$W"; mkdir -p "$W"
C="$(git -C "$REPO" rev-parse HEAD)"
hostinfo "$W/host.txt" before
t0=$(date -u +%s)
vmload_sampler "$W/vm-load.tsv"; S=$!
BENCH_COMMIT="$C" BENCH_DIRTY=false TSLS_SKIP_BUILD=1 \
  BENCH_MAX_CALIBRATION_MS="${BENCH_MAX_CALIBRATION_MS:-150}" \
  BENCH_HOST_NOTE="Apple M4 (macOS, Darwin 25.6.0) -> Lima 'tsls-kvm' vmType vz with nested virtualization, 4 vCPU / 8 GiB guest; the Mac is a shared developer machine (see physical-host-load.tsv)" \
  bash scripts/kvm/bench.sh > "$W/stdout.txt" 2>&1
rc=$?
kill $S 2>/dev/null
t1=$(date -u +%s)
echo "bench exit=$rc" | tee -a "$W/host.txt"
hostinfo "$W/host.txt" after
dir="$(ls -d "$REPO"/docs/evidence/bench-* 2>/dev/null | tail -n 1)"
echo "evidence: $dir" | tee -a "$W/host.txt"
if [ -n "$dir" ] && [ -f "$MACLOAD" ]; then
  awk -F'\t' -v a="$t0" -v b="$t1" 'NR > 1 && $1 >= a && $1 <= b {printf "%s\t{ %s %s %s }\n", $2, $3, $4, $5}' "$MACLOAD" > "$dir/physical-host-load.tsv"
  bash scripts/kvm/bench-report.sh "$dir" >> "$W/stdout.txt" 2>&1
fi
fix_owner "$W" "$REPO/docs/evidence" "$REPO/.kvm/run"
leftovers >> "$W/host.txt" 2>&1
tail -30 "$W/stdout.txt"
