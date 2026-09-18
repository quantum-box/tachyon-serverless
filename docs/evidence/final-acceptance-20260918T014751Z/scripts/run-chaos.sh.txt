#!/bin/bash
# R5 (PLT-4646 / PLT-4649): the failure matrix. PROVIDER=process runs every scenario as the
# ordinary user (root would defeat the chmod 000 of object_store_unavailable), PROVIDER=firecracker
# runs the ones whose behaviour or cleanup differs on a microVM, as root (jailer + cgroup).
. "$(dirname "$0")/common.sh"
PROVIDER="${PROVIDER:-process}"
ONLY="${ONLY:-}"
W=$RUNS/${RUNDIR:-chaos-$PROVIDER}; sudo rm -rf "$W"; mkdir -p "$W"
E="$W/matrix"
C="$(git -C "$REPO" rev-parse HEAD)"
hostinfo "$W/host.txt" before
vmload_sampler "$W/vm-load.tsv"; S=$!
if [ "$PROVIDER" = firecracker ]; then
  sudo -n env PATH="$PATH" HOME="$HOME" TMPDIR="$HOME/w" CHAOS_TMP="$HOME/w" TSLS_PROVIDER="$PROVIDER" \
    TSLS_SKIP_BUILD=1 TSLS_COMMIT="$C" \
    bash scripts/chaos/matrix.sh ${ONLY:+--only "$ONLY"} --retries "${RETRIES:-2}" --evidence "$E" > "$W/stdout.txt" 2>&1
else
  env TMPDIR="$HOME/w" CHAOS_TMP="$HOME/w" TSLS_PROVIDER="$PROVIDER" TSLS_SKIP_BUILD=1 TSLS_COMMIT="$C" \
    bash scripts/chaos/matrix.sh ${ONLY:+--only "$ONLY"} --retries "${RETRIES:-2}" --evidence "$E" > "$W/stdout.txt" 2>&1
fi
rc=$?
kill $S 2>/dev/null
echo "matrix($PROVIDER) exit=$rc commit=$C" | tee -a "$W/host.txt"
hostinfo "$W/host.txt" after
fix_owner "$W" "$REPO/.kvm/run"
leftovers >> "$W/host.txt" 2>&1
cat "$E/summary.md" 2>/dev/null
