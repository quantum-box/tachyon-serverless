#!/usr/bin/env bash
# scripts/queue/lib.sh - shared helpers for the local NATS JetStream queue (PLT-4638).
#
# Sourced by up.sh / down.sh / verify.sh. Downloads the pinned nats-server release named in
# deploy/nats/versions.env, verifies its sha256 before use, renders the server configuration and
# manages the process. No docker, no root.
#
# Environment:
#   QUEUE_STATE_DIR   state directory (default target/queue/nats): config, auth, store, pid, log
#   QUEUE_PORT        client port (default 14222, loopback)
#   QUEUE_HTTP_PORT   monitoring port (default 18222, loopback)
#   QUEUE_BIN_DIR     where the verified binary is kept (default target/queue/bin)
set -euo pipefail

QUEUE_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$QUEUE_LIB_DIR/../.." && pwd)"
QUEUE_STATE_DIR="${QUEUE_STATE_DIR:-$REPO_ROOT/target/queue/nats}"
QUEUE_PORT="${QUEUE_PORT:-14222}"
QUEUE_HTTP_PORT="${QUEUE_HTTP_PORT:-18222}"
QUEUE_BIN_DIR="${QUEUE_BIN_DIR:-$REPO_ROOT/target/queue/bin}"

# shellcheck source=deploy/nats/versions.env
. "$REPO_ROOT/deploy/nats/versions.env"

queue_log() { printf '[queue] %s\n' "$*" >&2; }
queue_die() { printf '[queue] ERROR: %s\n' "$*" >&2; exit 1; }

queue_platform() {
  local os arch
  case "$(uname -s)" in
    Darwin) os=darwin ;;
    Linux) os=linux ;;
    *) queue_die "unsupported OS $(uname -s)" ;;
  esac
  case "$(uname -m)" in
    arm64 | aarch64) arch=arm64 ;;
    x86_64 | amd64) arch=amd64 ;;
    *) queue_die "unsupported architecture $(uname -m)" ;;
  esac
  printf '%s-%s\n' "$os" "$arch"
}

queue_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

# queue_expected_sha PLATFORM -> the pinned checksum (empty when the platform is not pinned)
queue_expected_sha() {
  local var="NATS_SERVER_SHA256_${1//-/_}"
  printf '%s\n' "${!var:-}"
}

# queue_binary -> path of a verified nats-server binary (downloads it on first use)
queue_binary() {
  local platform expected name bin tarball url actual tmp
  platform="$(queue_platform)"
  expected="$(queue_expected_sha "$platform")"
  [ -n "$expected" ] || queue_die "no pinned checksum for $platform in deploy/nats/versions.env"
  name="nats-server-${NATS_SERVER_VERSION}-${platform}"
  bin="$QUEUE_BIN_DIR/$name/nats-server"
  tarball="$QUEUE_BIN_DIR/$name.tar.gz"
  if [ -x "$bin" ] && [ -f "$tarball" ] && [ "$(queue_sha256 "$tarball")" = "$expected" ]; then
    printf '%s\n' "$bin"
    return 0
  fi
  mkdir -p "$QUEUE_BIN_DIR"
  url="https://github.com/nats-io/nats-server/releases/download/${NATS_SERVER_VERSION}/${name}.tar.gz"
  tmp="$(mktemp "$QUEUE_BIN_DIR/.download.XXXXXX")"
  queue_log "downloading $url"
  curl -fsSL --retry 3 -o "$tmp" "$url" || { rm -f "$tmp"; queue_die "download failed: $url"; }
  actual="$(queue_sha256 "$tmp")"
  if [ "$actual" != "$expected" ]; then
    rm -f "$tmp"
    queue_die "sha256 mismatch for $name.tar.gz: expected $expected, got $actual (refusing to run it)"
  fi
  mv "$tmp" "$tarball"
  rm -rf "${QUEUE_BIN_DIR:?}/$name"
  tar -xzf "$tarball" -C "$QUEUE_BIN_DIR"
  [ -x "$bin" ] || queue_die "archive did not contain $name/nats-server"
  queue_log "verified $name (sha256 $actual)"
  printf '%s\n' "$bin"
}

queue_random_password() {
  od -An -N24 -tx1 /dev/urandom | tr -d ' \n'
}

# queue_render: write nats-server.conf, auth.conf and gateway.password into the state directory.
# An existing password is kept, so a restart keeps the credentials the gateway already has.
queue_render() {
  local state="$QUEUE_STATE_DIR" password
  umask 077
  mkdir -p "$state/jetstream"
  chmod 700 "$state" "$state/jetstream"
  if [ ! -s "$state/gateway.password" ]; then
    queue_random_password >"$state/gateway.password"
  fi
  chmod 600 "$state/gateway.password"
  password="$(cat "$state/gateway.password")"
  sed -e "s|@PASSWORD@|$password|" "$REPO_ROOT/deploy/nats/auth.conf.template" >"$state/auth.conf"
  chmod 600 "$state/auth.conf"
  sed -e "s|@PORT@|$QUEUE_PORT|" \
    -e "s|@HTTP_PORT@|$QUEUE_HTTP_PORT|" \
    -e "s|@STORE_DIR@|$state/jetstream|" \
    "$REPO_ROOT/deploy/nats/nats-server.conf" >"$state/nats-server.conf"
  chmod 600 "$state/nats-server.conf"
}

queue_pid() {
  local pidfile="$QUEUE_STATE_DIR/nats-server.pid"
  [ -f "$pidfile" ] || return 1
  local pid
  pid="$(cat "$pidfile")"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || return 1
  printf '%s\n' "$pid"
}

# queue_wait_ready SECONDS: JetStream answers /healthz?js-enabled-only=true
queue_wait_ready() {
  local deadline=$((SECONDS + ${1:-30}))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS "http://127.0.0.1:$QUEUE_HTTP_PORT/healthz?js-enabled-only=true" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

queue_env() {
  printf 'export TACHYON_NATS_URL=nats://127.0.0.1:%s\n' "$QUEUE_PORT"
  printf 'export TACHYON_NATS_USER=gateway\n'
  printf 'export TACHYON_NATS_PASSWORD_FILE=%s\n' "$QUEUE_STATE_DIR/gateway.password"
}
