#!/bin/bash
# R8 (PLT-4640 / PLT-4641 / PLT-4649): async dispatch + cron/webhook triggers on Firecracker
. "$(dirname "$0")/common.sh"
W=$RUNS/queue; mkdir -p "$W"
C="$(git -C "$REPO" rev-parse HEAD)"
for what in ${*:-dispatch triggers}; do
  case "$what" in
    dispatch) script=scripts/queue/async-dispatch-e2e.sh ;;
    triggers) script=scripts/queue/triggers-e2e.sh ;;
  esac
  E="$W/$what"; sudo rm -rf "$E"; mkdir -p "$E"
  hostinfo "$W/host-$what.txt" before
  vmload_sampler "$W/vm-load-$what.tsv"; S=$!
  sudo -n env PATH="$PATH" HOME="$HOME" TMPDIR="$HOME/w" TSLS_PROVIDER=firecracker TSLS_SKIP_BUILD=1 \
    TSLS_COMMIT="$C" bash "$script" --evidence "$E" > "$W/stdout-$what.txt" 2>&1
  rc=$?
  kill $S 2>/dev/null
  echo "$what exit=$rc" | tee -a "$W/host-$what.txt"
  hostinfo "$W/host-$what.txt" after
  leftovers >> "$W/host-$what.txt" 2>&1
  fix_owner "$W" "$REPO/.kvm/run"
  grep -c ' ok ' "$E/results.txt" 2>/dev/null; grep ' FAIL ' "$E/results.txt" 2>/dev/null
done
