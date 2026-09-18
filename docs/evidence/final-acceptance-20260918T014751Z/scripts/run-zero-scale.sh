#!/bin/bash
# R3a (PLT-4635 / PLT-4649): scale to zero, cold re-access, alias switch drain, deletion
. "$(dirname "$0")/common.sh"
W=$RUNS/zero-scale; sudo rm -rf "$W"; mkdir -p "$W"
cat > "$W/extra.toml" <<'TOML'

# PLT-4649 final acceptance (same settings as the PLT-4635 KVM verification).
[pool]
enabled = true

[scaling]
reconcile_interval_ms = 500
drain_timeout_seconds = 5
allow_short_drain = true
TOML
mkcfg "$W/gateway.toml" 18483 "$W/data" "$W/extra.toml"
sed -i 's/^queue_timeout_seconds = .*/queue_timeout_seconds = 60/' "$W/gateway.toml"
hostinfo "$W/host.txt" before
vmload_sampler "$W/vm-load.tsv"; S=$!
sudo -n env PATH="$PATH" HOME="$HOME" TMPDIR=/tmp TSLS_PROVIDER=firecracker TSLS_SKIP_BUILD=1 \
  TSLS_GATEWAY_CONFIG="$W/gateway.toml" TSLS_API_URL=http://127.0.0.1:18483 \
  TSLS_TOKEN_A=dev-token-tenant-a TSLS_GUEST_DIR="$REPO/target/aarch64-unknown-linux-musl/release" \
  TSLS_ARCH=aarch64 bash scripts/e2e/zero-scale.sh > "$W/stdout.txt" 2>&1
rc=$?
kill $S 2>/dev/null
echo "zero-scale exit=$rc" | tee -a "$W/host.txt"
hostinfo "$W/host.txt" after
fix_owner "$REPO/docs/evidence" "$W" "$REPO/.kvm/run"
leftovers >> "$W/host.txt" 2>&1
grep -E "^(PASS|FAIL|NOTE|SKIP)" "$W/stdout.txt" | tail -25
