#!/bin/bash
# R6 (PLT-4642 / PLT-4643 / PLT-4649): usage journal -> collector -> ledger (counted once) and budget
. "$(dirname "$0")/common.sh"
W=$RUNS/usage; mkdir -p "$W"
C="$(git -C "$REPO" rev-parse HEAD)"
for what in ${*:-usage budget}; do
  E="$W/$what"; sudo rm -rf "$E"; mkdir -p "$E"
  hostinfo "$W/host-$what.txt" before
  vmload_sampler "$W/vm-load-$what.tsv"; S=$!
  sudo -n env PATH="$PATH" HOME="$HOME" TMPDIR="$HOME/w" TSLS_PROVIDER=firecracker TSLS_SKIP_BUILD=1 \
    TSLS_COMMIT="$C" TSLS_EVIDENCE_DIR="$E" bash "scripts/usage/$what-e2e.sh" > "$W/stdout-$what.txt" 2>&1
  rc=$?
  kill $S 2>/dev/null
  echo "$what exit=$rc" | tee -a "$W/host-$what.txt"
  hostinfo "$W/host-$what.txt" after
  fix_owner "$W" "$REPO/.kvm/run"
  leftovers >> "$W/host-$what.txt" 2>&1
  grep -h -E '^(PASS|FAIL|note)' "$E"/*/summary.txt 2>/dev/null | tail -30
done
