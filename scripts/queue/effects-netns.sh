#!/usr/bin/env bash
# scripts/queue/effects-netns.sh - an "external" side-effect store for examples/idempotent-async that
# Firecracker guests can reach through egress `restricted` (Linux, root; test infrastructure only).
#
#   scripts/queue/effects-netns.sh up DIR [PORT]   -> prints IDEMPOTENT_ASYNC_URL=... and EFFECTS_ALLOW=...
#   scripts/queue/effects-netns.sh down
#   scripts/queue/effects-netns.sh status          -> exit 1 when anything of it is left
#
# Why a network namespace: the provider's nftables table drops everything a guest sends to the node
# itself (`input`) and every special-purpose IPv4 range (RFC 1918, loopback, CGNAT, TEST-NET, ...),
# and deploy refuses such an allowlist (docs/adr/0005-egress-profiles.md). The store therefore
# listens in its own namespace behind a veth pair on EFFECTS_NET (default 192.31.196.0/30, part of
# the AS112 sink range, which a lab host never needs), so a guest reaches it the way it would reach
# an external database: tap -> forward -> masquerade -> veth. The address is routed only inside
# this host; nothing leaves it.
#
# `up` enables net.ipv4.ip_forward (the restricted profile needs it) and records the previous value;
# `down` stops the store, deletes the namespace and the veth, and restores ip_forward.
# Names never start with `tsls` (the provider's reconcile removes unknown tsls* links).
set -euo pipefail

NS="${EFFECTS_NS:-effx}"
VETH_HOST="${NS}0"
VETH_NS="${NS}1"
NET="${EFFECTS_NET:-192.31.196.0/30}"
BASE="${NET%/*}"
PREFIX="${BASE%.*}"
LAST="${BASE##*.}"
HOST_IP="$PREFIX.$((LAST + 1))"
STORE_IP="$PREFIX.$((LAST + 2))"
STATE="${EFFECTS_STATE_DIR:-/run/tsls-effects-$NS}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

[ "$(id -u)" = 0 ] || { echo "effects-netns.sh needs root (ip netns, veth, sysctl)" >&2; exit 2; }

cmd="${1:-}"
case "$cmd" in
  up)
    dir="${2:?usage: effects-netns.sh up DIR [PORT]}"
    port="${3:-8787}"
    mkdir -p "$STATE" "$dir"
    if ip netns list | awk '{print $1}' | grep -qx "$NS"; then
      echo "namespace $NS already exists (run: effects-netns.sh down)" >&2
      exit 1
    fi
    [ -f "$STATE/ip_forward" ] || sysctl -n net.ipv4.ip_forward >"$STATE/ip_forward"
    sysctl -qw net.ipv4.ip_forward=1
    ip netns add "$NS"
    ip link add "$VETH_HOST" type veth peer name "$VETH_NS"
    ip link set "$VETH_NS" netns "$NS"
    ip addr add "$HOST_IP/30" dev "$VETH_HOST"
    ip link set "$VETH_HOST" up
    ip netns exec "$NS" ip addr add "$STORE_IP/30" dev "$VETH_NS"
    ip netns exec "$NS" ip link set "$VETH_NS" up
    ip netns exec "$NS" ip link set lo up
    ip netns exec "$NS" ip route add default via "$HOST_IP"
    ip netns exec "$NS" python3 "$SCRIPT_DIR/effects-store.py" --dir "$dir" --bind "$STORE_IP" --port "$port" \
      >"$STATE/store.log" 2>&1 &
    echo $! >"$STATE/store.pid"
    for _ in $(seq 1 40); do
      if grep -q 'listening' "$STATE/store.log" 2>/dev/null; then break; fi
      sleep 0.1
    done
    grep -q 'listening' "$STATE/store.log" || { cat "$STATE/store.log" >&2; exit 1; }
    echo "IDEMPOTENT_ASYNC_URL=http://$STORE_IP:$port"
    echo "EFFECTS_ALLOW=$STORE_IP/32:$port"
    echo "EFFECTS_STORE_LOG=$STATE/store.log"
    ;;
  down)
    if [ -f "$STATE/store.pid" ]; then
      kill "$(cat "$STATE/store.pid")" 2>/dev/null || true
    fi
    ip netns pids "$NS" 2>/dev/null | xargs -r kill -KILL 2>/dev/null || true
    ip link del "$VETH_HOST" 2>/dev/null || true
    ip netns del "$NS" 2>/dev/null || true
    if [ -f "$STATE/ip_forward" ]; then
      sysctl -qw "net.ipv4.ip_forward=$(cat "$STATE/ip_forward")"
    fi
    rm -rf "$STATE"
    ;;
  status)
    left=0
    if ip netns list | awk '{print $1}' | grep -qx "$NS"; then echo "namespace $NS"; left=1; fi
    if ip link show "$VETH_HOST" >/dev/null 2>&1; then echo "link $VETH_HOST"; left=1; fi
    if [ -d "$STATE" ]; then echo "state $STATE"; left=1; fi
    echo "ip_forward=$(sysctl -n net.ipv4.ip_forward)"
    exit "$left"
    ;;
  *)
    sed -n '2,20p' "$0" >&2
    exit 2
    ;;
esac
