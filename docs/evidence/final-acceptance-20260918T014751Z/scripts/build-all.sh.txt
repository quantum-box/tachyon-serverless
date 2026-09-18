#!/bin/bash
# Build everything the PLT-4649 runs need (the run scripts then use TSLS_SKIP_BUILD=1).
set -euo pipefail
source ~/.cargo/env 2>/dev/null || true
cd ~/lab
df -h / | tail -1
A=aarch64-unknown-linux-musl
cargo build -p tachyon-serverless-gateway --features failpoints
cargo build -p tachyon-serverless-cli -p tachyon-serverless-load -p tachyon-serverless-runtime-bridge
cargo build -p tachyon-serverless-queue-nats --bin tachyon-queue-probe
cargo build --release -p tachyon-serverless-gateway -p tachyon-serverless-cli
cargo build --release --target $A -p tachyon-serverless-runtime-bridge -p example-hello \
  -p example-http-axum -p example-cpu-burn -p example-isolation-probe -p example-idempotent-async
scripts/kvm/build-rootfs.sh
df -h / | tail -1
echo BUILD-DONE
