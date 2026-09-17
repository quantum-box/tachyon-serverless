#!/usr/bin/env bash
# scripts/lab/lib.sh - shared helpers of scripts/lab/lab.sh (PLT-4648). Sourced, never executed.
#
# bash 3.2 compatible (macOS /bin/bash): no associative arrays, no mapfile, no ${var,,}.
# Values read by the sourcing scripts are assigned here (file-wide SC2034 exemption).
# shellcheck disable=SC2034

LAB_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$LAB_LIB_DIR/../.." && pwd)"
LOCK_FILE="$REPO_ROOT/deploy/lab/versions.lock"
# shellcheck source=deploy/lab/versions.lock
. "$LOCK_FILE"

# ---------------------------------------------------------------------------
# output
# ---------------------------------------------------------------------------

lab_log() { printf '[lab] %s\n' "$*"; }
lab_warn() { printf '[lab] WARN: %s\n' "$*"; }
lab_die() { printf '[lab] ERROR: %s\n' "$*"; exit 1; }
# lab_cmd CMD... : print the command (never an environment variable value) and run it.
lab_cmd() { printf '+ %s\n' "$*"; "$@"; }
lab_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
lab_stamp() { date -u +%Y%m%dT%H%M%SZ; }
have() { command -v "$1" >/dev/null 2>&1; }

# ---------------------------------------------------------------------------
# host facts
# ---------------------------------------------------------------------------

host_os() { uname -s; }
host_arch() { # aarch64 | x86_64 | other
  case "$(uname -m)" in
    arm64 | aarch64) echo aarch64 ;;
    x86_64 | amd64) echo x86_64 ;;
    *) uname -m ;;
  esac
}

sha256_of() {
  if have sha256sum; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi
}

# file_mode PATH -> octal permission bits, e.g. 600
file_mode() {
  if stat -c %a "$1" >/dev/null 2>&1; then stat -c %a "$1"; else stat -f %Lp "$1"; fi
}

# version_ge A B : A >= B for dotted versions (sort -V is not on every macOS, so compare fields)
version_ge() {
  local a="$1" b="$2" x y i=1
  while :; do
    x="$(printf '%s' "$a" | cut -d. -f"$i")"
    y="$(printf '%s' "$b" | cut -d. -f"$i")"
    [ -n "$x$y" ] || return 0
    x="${x%%[!0-9]*}"; y="${y%%[!0-9]*}"
    x="${x:-0}"; y="${y:-0}"
    if [ "$x" -gt "$y" ]; then return 0; fi
    if [ "$x" -lt "$y" ]; then return 1; fi
    i=$((i + 1))
    [ "$i" -le 6 ] || return 0
  done
}

# free_mib DIR -> free MiB of the filesystem holding DIR (nearest existing parent)
free_mib() {
  local d="$1"
  while [ ! -d "$d" ]; do d="$(dirname "$d")"; done
  df -Pk "$d" | awk 'NR==2 {printf "%d\n", $4 / 1024}'
}

total_memory_mib() {
  if [ -r /proc/meminfo ]; then
    awk '/^MemTotal:/ {printf "%d\n", $2 / 1024}' /proc/meminfo
  elif have sysctl; then
    echo $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 ))
  else
    echo 0
  fi
}

# port_free PORT -> 0 when nothing listens on 127.0.0.1:PORT
port_free() {
  python3 - "$1" <<'PY'
import socket, sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
}

pick_free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

random_hex() { od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'; }

# abs_dir DIR -> physical absolute path of an existing directory (symlinks resolved)
abs_dir() { (cd -P "$1" 2>/dev/null && pwd -P); }

# ---------------------------------------------------------------------------
# lab directory and manifest
# ---------------------------------------------------------------------------

# lab_paths: derive every path of the lab from LAB_DIR (must already be absolute).
lab_paths() {
  MANIFEST="$LAB_DIR/manifest.env"
  MARKER="$LAB_DIR/.tsls-lab"
  LOG_DIR="$LAB_DIR/logs"
  CMD_LOG_DIR="$LOG_DIR/commands"
  SECRETS_DIR="$LAB_DIR/secrets"
  TOKENS_FILE="$SECRETS_DIR/tokens.env"
  OBJECT_KEY="$SECRETS_DIR/object.key"
  TRIGGERS_KEY="$SECRETS_DIR/triggers.key"
  CONFIG_DIR="$LAB_DIR/config"
  GATEWAY_CONFIG="$CONFIG_DIR/gateway.toml"
  BUDGET_FILE="$CONFIG_DIR/budgets.toml"
  DATA_DIR="$LAB_DIR/data"
  OBJECTS_ROOT="$DATA_DIR/objects"
  NATS_DIR="$LAB_DIR/nats"
  CACHE_DIR="$LAB_DIR/cache"
  NATS_BIN_DIR="$CACHE_DIR/nats"
  KVM_CACHE="$CACHE_DIR/kvm"
  FC_RUN_DIR="$LAB_DIR/fc/run"
  FC_JAIL_DIR="$LAB_DIR/fc/jail"
  RUN_DIR="$LAB_DIR/run"
  GATEWAY_PID_FILE="$RUN_DIR/gateway.pid"
  GATEWAY_LOG="$LOG_DIR/gateway.log"
  DEMO_DIR="$LAB_DIR/demo"
}

manifest_get() { # KEY -> value (empty when absent)
  [ -f "$MANIFEST" ] || return 0
  sed -n "s/^$1=//p" "$MANIFEST" | tail -n 1
}

manifest_set() { # KEY VALUE (VALUE must not contain a newline)
  local tmp
  mkdir -p "$LAB_DIR"
  tmp="$MANIFEST.tmp.$$"
  {
    if [ -f "$MANIFEST" ]; then grep -v "^$1=" "$MANIFEST" || true; fi
    printf '%s=%s\n' "$1" "$2"
  } >"$tmp"
  mv "$tmp" "$MANIFEST"
}

# foreign_content DIR -> entries of DIR other than the command log directory a first command creates
foreign_content() {
  local p
  for p in "$1"/* "$1"/.[!.]*; do
    [ -e "$p" ] || [ -L "$p" ] || continue
    [ "$(basename "$p")" = logs ] || printf '%s\n' "$p"
  done
}

# lab_init: create the lab directory, its ownership marker and the manifest (idempotent).
lab_init() {
  local id
  if [ -d "$LAB_DIR" ] && [ ! -f "$MARKER" ] && [ -n "$(foreign_content "$LAB_DIR")" ]; then
    lab_die "$LAB_DIR exists, is not empty and has no .tsls-lab marker: refusing to use it as a lab directory"
  fi
  mkdir -p "$LAB_DIR" "$CMD_LOG_DIR"
  if [ ! -f "$MARKER" ]; then
    id="lab-$(random_hex 4)"
    printf '%s\n' "$id" >"$MARKER"
    manifest_set LAB_ID "$id"
    manifest_set CREATED_AT "$(lab_now)"
    manifest_set REPO_ROOT "$REPO_ROOT"
    manifest_set HOST "$(uname -srm)"
    manifest_set STATE initialized
  fi
  LAB_ID="$(cat "$MARKER")"
  [ "$(manifest_get LAB_ID)" = "$LAB_ID" ] || lab_die "manifest LAB_ID differs from $MARKER; the lab directory was edited by hand"
}

lab_require_init() {
  if [ ! -f "$MARKER" ] || [ ! -f "$MANIFEST" ]; then
    lab_die "no lab at $LAB_DIR (run: scripts/lab/lab.sh bootstrap)"
  fi
  LAB_ID="$(cat "$MARKER")"
}

# lab_provider: resolve the provider from --provider, the manifest, or the default (process).
lab_provider() {
  local recorded
  recorded="$(manifest_get PROVIDER)"
  if [ -n "${OPT_PROVIDER:-}" ]; then
    if [ -n "$recorded" ] && [ "$recorded" != "$OPT_PROVIDER" ]; then
      lab_die "this lab was created for provider '$recorded'; use another --lab-dir for '$OPT_PROVIDER'"
    fi
    PROVIDER="$OPT_PROVIDER"
  else
    PROVIDER="${recorded:-process}"
  fi
  case "$PROVIDER" in
    process | firecracker) ;;
    *) lab_die "--provider must be process or firecracker (got $PROVIDER)" ;;
  esac
}

# ---------------------------------------------------------------------------
# binaries
# ---------------------------------------------------------------------------

lab_binaries() {
  ARCH="$(host_arch)"
  GATEWAY_BIN="$REPO_ROOT/target/debug/tachyon-serverless-gateway"
  TSLS_BIN="$REPO_ROOT/target/debug/tsls"
  if [ "$PROVIDER" = firecracker ]; then
    GUEST_DIR="$REPO_ROOT/target/$ARCH-unknown-linux-musl/release"
    BRIDGE_BIN="$GUEST_DIR/tachyon-serverless-runtime-bridge"
    FC_BIN="$KVM_CACHE/bin/firecracker"
    JAILER_BIN="$KVM_CACHE/bin/jailer"
    FC_KERNEL="$KVM_CACHE/vmlinux"
    FC_ROOTFS="$KVM_CACHE/rootfs.ext4"
  else
    GUEST_DIR="$REPO_ROOT/target/debug"
    BRIDGE_BIN="$REPO_ROOT/target/debug/tachyon-serverless-runtime-bridge"
  fi
}

# ---------------------------------------------------------------------------
# processes
# ---------------------------------------------------------------------------

# pid_alive PIDFILE -> prints the pid when the process is alive
pid_alive() {
  local pid
  [ -f "$1" ] || return 1
  pid="$(cat "$1" 2>/dev/null)"
  [ -n "$pid" ] || return 1
  if kill -0 "$pid" 2>/dev/null || { [ "${LAB_SUDO:-}" != "" ] && $LAB_SUDO kill -0 "$pid" 2>/dev/null; }; then
    printf '%s\n' "$pid"
    return 0
  fi
  return 1
}

# lab_processes -> `pid command` lines of every process whose command line names the lab directory
lab_processes() {
  # The lab's own transcript writer names logs/commands/ and is not a leftover.
  # The directory is passed to awk through the environment so that awk itself never matches.
  ps -axo pid=,command= 2>/dev/null | LAB_MATCH="$LAB_DIR/" awk 'index($0, ENVIRON["LAB_MATCH"]) > 0' \
    | grep -v -e 'scripts/lab/lab.sh' -e "$LAB_DIR/logs/" || true
}

# ---------------------------------------------------------------------------
# tokens and HTTP
# ---------------------------------------------------------------------------

lab_load_tokens() {
  [ -f "$TOKENS_FILE" ] || lab_die "no $TOKENS_FILE (run: scripts/lab/lab.sh up)"
  # shellcheck disable=SC1090
  . "$TOKENS_FILE"
  API="http://127.0.0.1:$(manifest_get GATEWAY_PORT)"
}

HTTP_CODE=""
HTTP_BODY=""
# api TOKEN METHOD PATH [BODY_FILE] -> HTTP_CODE, HTTP_BODY. Prints the request line, never the token.
api() {
  local token="$1" method="$2" path="$3" body="${4:-}" out args
  out="$(mktemp "${TMPDIR:-/tmp}/lab-http.XXXXXX")"
  args=(-s -o "$out" -w '%{http_code}' --max-time 60 -X "$method")
  if [ -n "$token" ]; then args+=(-H "authorization: Bearer $token"); fi
  if [ -n "$body" ]; then args+=(-H 'content-type: application/json' --data-binary "@$body"); fi
  printf '+ curl -X %s %s%s%s\n' "$method" "$API" "$path" "${body:+ --data-binary @$body}"
  HTTP_CODE="$(curl "${args[@]}" "$API$path" || true)"
  HTTP_BODY="$(cat "$out")"
  rm -f "$out"
}

jqb() { printf '%s' "$HTTP_BODY" | jq -r "$1" 2>/dev/null; }

# sqlite_ro DB SQL -> rows as a|b|c (python3's sqlite3 module; no sqlite3 CLI needed). A privileged
# firecracker lab runs the gateway as root, so its data files are root-owned (0600): when the read
# fails and LAB_SUDO is set, read again through it.
sqlite_ro() {
  sqlite_ro_as "" "$1" "$2" 2>/dev/null || { [ -n "${LAB_SUDO:-}" ] && sqlite_ro_as "$LAB_SUDO" "$1" "$2"; }
}
sqlite_ro_as() { # sqlite_ro_as SUDO_OR_EMPTY DB SQL
  ${1:+$1 }python3 - "$2" "$3" <<'PY'
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True, timeout=10)
for row in con.execute(sys.argv[2]):
    print("|".join("" if v is None else str(v) for v in row))
PY
}
