PLT-4651 (restore-aware example, PR #10) on Firecracker, cold path - KVM verification 2026-09-17

Source: origin/main b5a7e54, guest binaries and rootfs rebuilt from it (musl release,
example-restore-aware 1539848 bytes). Host: Lima VM (nested virtualization), Firecracker / jailer
v1.17.0; gateway as root with config/gateway.firecracker.toml (listen / data_dir changed,
gateway.toml). macOS 1-min load average 5.2 -> 4.9.

Ran as: run-restore-aware.sh.txt (deploy three revisions of examples/restore-aware, invoke each through
the CLI, fetch GET /v1/invocations/<id> and the logs, stop the gateway, check leftovers).
summary.txt lists revisions and CLI exit codes.

Result:
- {"n":97}: exit 0, is_prime true, restored false, generation 0, table_primes 9592,
  secret_present true, per-instance instance_id / connection_id / random. Attempt cold,
  environment_boot_ms 5294, runtime_init_ms 597, handler_ms 120. Logs:
  "[stderr/init] restore-aware: bootstrap: building the lookup table (no runtime, no secrets)"
  -> "[platform/init] lifecycle continue answered (cold)" -> "[stderr/init] restore-aware:
  after_restore: restored=false generation=0 instance=env_...".
- {"n":91}: exit 0, is_prime false, a different instance_id and random value on a new microVM.
- RESTORE_AWARE_FAIL=bootstrap: exit 3, init_error, Runtime.PreCheckpointFailed (the hook failed
  before the checkpoint; no "continue" line in the log).
- RESTORE_AWARE_FAIL=after_restore: exit 3, init_error, Runtime.AfterRestoreFailed, after
  "lifecycle continue answered (cold)".
The failed invocations have no attempt row (attempts: []), which is how every init failure is
recorded today (the demo's init_error invocation has none either); not specific to this example.
After the gateway stopped: no VMM / jailer / gateway process, cgroup, jail, tap or tachyon nft
table (host.txt).
Not shown: a restored start (no provider takes snapshots) and the lifecycle timeouts on KVM.
