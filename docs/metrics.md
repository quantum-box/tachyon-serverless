# 再利用・スケールの metrics と負荷シナリオ（PLT-4637）

- 対象: `GET /metrics`（`crates/application/src/metrics/`、`apps/gateway`）、`GET /v1/capacity` の `reuse`、`deploy/prometheus/alerts.yml`、負荷ハーネス `apps/load`（`tsls-load`）と `scripts/load/scenarios.sh`
- 決定の記録: [adr/0011-reuse-and-scaling-metrics.md](adr/0011-reuse-and-scaling-metrics.md)
- 関連: [adr/0006](adr/0006-autoscaling-and-admission.md)（admission）、[adr/0009](adr/0009-scale-to-zero-and-drain.md)（zero-scale / drain）、[adr/0007](adr/0007-config-distribution-and-auth-leases.md)（設定 cache）、[api.md](api.md)、[acceptance.md](acceptance.md) §PLT-4637
- **数値はすべて 1 回の実行・1 台の観測値で、SLA ではない。**

## 1. 何を観測するか

再利用（warm pool）と自動増減（admission / zero-scale）が実際に成立しているかを、回帰として検出できる形で出す。

| 問い | 見る metric / 仕組み |
|---|---|
| 0 → 負荷増 → 上限 → 減少 → 0 → 再起動 が起きたか | `tsls_environments{state}`・`tsls_node_in_flight`・`tsls_queue_length` の時系列（負荷ハーネスの `samples.jsonl` と `timeline.svg`） |
| 同じ環境を再利用したか | `tsls_boot_identity_checks_total{result}`（boot id）、`tsls_attempts_total{start_kind}`、`GET /v1/capacity` の `reuse` |
| 再利用しない profile か | `tsls_environment_reuse_mode{mode="every_invocation_boots"} 1`、`reuse.mode` |
| 起動予約の超過 | `tsls_node_reserved_*` と `tsls_node_capacity_*`、`tsls_node_in_flight` と `tsls_node_max_concurrency`、revision / tenant の cap（§5 overshoot） |
| starvation | `tsls_tenant_queue_oldest_age_seconds` と他 tenant の `tsls_tenant_grants_total`（§5 starvation） |
| idle 中の想定外の資源利用 | `tsls_idle_environment_cpu_ratio_max`、`tsls_environment_cpu_seconds_total{state="idle"}`（§5 idle CPU） |
| 起動の合流・失敗 | `tsls_admission_coalesced_waits_total`、`tsls_admission_starts_avoided_total`、`tsls_environment_starts_total{result}`、`tsls_circuit_breaker_opens_total` |

## 2. `GET /metrics`

- Prometheus text exposition 0.0.4（`Content-Type: text/plain; version=0.0.4; charset=utf-8`、`Cache-Control: no-store`）。依存を増やさない手書きの registry（`crates/application/src/metrics/`）。
- **認証**: `[metrics] bearer_token`（16 bytes 以上、定数時間比較）を `Authorization: Bearer` で渡す。
  - 未設定なら route は存在しない（404）。
  - tenant の token（`[[identity.tokens]]`、operator role を含む）は 401。設定検証で tenant の token や `[control_plane] internal_token` と同じ値は拒否する。
  - 理由: metrics には全 tenant の tenant id・revision id・待ち時間が載る。tenant ごとの認証（operator role は自 tenant に閉じる、`docs/threat-model.md` §7）では扱えないので、platform operator 専用の credential を別に持つ。別 listener にはしていない（listen は gateway と同じ。外部に出す場合は reverse proxy で `/metrics` を塞ぐか token を運用する。T34）。
- **cardinality**: tenant / revision / environment の label は operator 向けの endpoint にだけ出し、件数に上限を持つ。
  - `[metrics] max_revision_series`（既定 64）: 環境数 + 待機数の多い順に残し、残りは `tenant="_other",revision="_other"` の 1 系列に合算（breaker は最も悪い状態）。
  - `[metrics] max_tenant_series`（既定 32）: 待機数・in-flight・grant 数の多い順。残りは `tenant="_other"`（合計、最古の待ちは最大値）。
  - `[metrics] max_environment_series`（既定 128）: 環境ごとの host 使用量の系列。idle CPU の計算は全環境で行う。
  - 畳んだ件数は `tsls_metrics_series_truncated{dimension}`。
- **scrape が sample を取る**: 1 回の scrape で、この dispatcher が持つ live 環境（最大 1024）ごとに provider の `environment_stats` を 1 回読む。idle CPU は前回の scrape との差分で出す（scrape 間隔が測定窓）。
- `GET /v1/capacity`（tenant の token）は従来どおり自 tenant の revision だけを返し、node 全体の `reuse` だけを足す（§4）。

```sh
curl -H "authorization: Bearer $METRICS_TOKEN" http://127.0.0.1:8080/metrics
```

```toml
[metrics]
bearer_token = "load-metrics-operator-token"   # 16 bytes 以上。tenant token と別の値
max_revision_series = 64
max_tenant_series = 32
max_environment_series = 128
```

Prometheus の scrape 設定例:

```yaml
scrape_configs:
  - job_name: tachyon-serverless
    scrape_interval: 5s
    authorization:
      credentials_file: /etc/prometheus/tsls-metrics.token
    static_configs:
      - targets: ["127.0.0.1:8080"]
rule_files:
  - deploy/prometheus/alerts.yml
```

## 3. metric catalog

単位は名前に入れる（`_seconds`、`_bytes`、`_millicores`、`_total`）。一覧の正は `crates/application/src/metrics/catalog.rs::FAMILIES` で、ここに載っていない family があると `metrics::tests::every_metric_family_is_documented` が失敗する。

### 3.1 build / provider / pool

| family | type | labels | 意味 |
|---|---|---|---|
| `tsls_build_info` | gauge | `version` | 常に 1 |
| `tsls_environment_reuse_mode` | gauge | `provider`, `mode` | 有効な mode が 1: `warm_reuse`（pool の環境を再利用）/ `every_invocation_boots`（warm の段階が無く、毎回起動。process provider は常にこれ） |
| `tsls_pool_reuse_enabled` | gauge | | provider の idle capability と `[pool] enabled` が再利用を許すとき 1 |
| `tsls_pool_held_environments` | gauge | | pool が bridge session を持つ環境（idle と quiesce 中） |
| `tsls_pool_quiescing_environments` | gauge | | pool に入る途中（quiesce 中） |

### 3.2 node・予約・環境の状態

| family | type | labels | 意味 |
|---|---|---|---|
| `tsls_node_info` | gauge | `node` | 常に 1 |
| `tsls_node_capacity_cpu_millicores` | gauge | | `[capacity.node] cpu_millis`。無制限なら系列なし |
| `tsls_node_capacity_memory_bytes` | gauge | | 同 memory。無制限なら系列なし |
| `tsls_node_capacity_ephemeral_storage_bytes` | gauge | | 同 ephemeral storage |
| `tsls_node_reserved_cpu_millicores` | gauge | | 予約の合計（starting・busy・parking・idle・draining、overhead 込み） |
| `tsls_node_reserved_memory_bytes` | gauge | | 同 memory |
| `tsls_node_reserved_ephemeral_storage_bytes` | gauge | | 同 storage |
| `tsls_node_environment_overhead_memory_bytes` | gauge | | 1 環境の VMM + bridge の overhead（推定値、ADR-0006） |
| `tsls_node_max_concurrency` | gauge | | `[capacity] max_concurrency` |
| `tsls_node_in_flight` | gauge | | promised + starting + busy |
| `tsls_environments` | gauge | `state` | node 全体の状態別の環境数（`promised` `starting` `busy` `parking` `idle` `draining`）。**Ready（すぐ使える）は `idle`**（ADR-0009 §10） |
| `tsls_revision_environments` | gauge | `tenant`, `revision`, `state` | revision ごとの状態別 |
| `tsls_revision_desired_environments` | gauge | `tenant`, `revision` | autoscaler の desired |
| `tsls_revision_max_environments` | gauge | `tenant`, `revision` | revision の `max_concurrency` |
| `tsls_revision_min_ready` | gauge | `tenant`, `revision` | `min_ready` |
| `tsls_revision_queue_length` | gauge | `tenant`, `revision` | revision の待機数 |
| `tsls_revision_circuit_breaker_state` | gauge | `tenant`, `revision` | 0 closed / 1 half_open / 2 open |

revision の系列は admission が revision を覚えている間だけ出る（0 になり到着率が減衰すると忘れる。ADR-0006）。counter は忘れない（§3.4）。

### 3.3 queue・tenant

| family | type | labels | 意味 |
|---|---|---|---|
| `tsls_queue_length` | gauge | | 待機数 |
| `tsls_queue_bytes` | gauge | | 待機中の payload bytes |
| `tsls_queue_max_length` | gauge | | `[capacity] max_queue` |
| `tsls_queue_max_bytes` | gauge | | `[capacity] max_queue_bytes` |
| `tsls_queue_oldest_age_seconds` | gauge | | 最古の待機者の待ち時間（空なら 0） |
| `tsls_tenant_queue_length` | gauge | `tenant` | tenant の待機数 |
| `tsls_tenant_queue_oldest_age_seconds` | gauge | `tenant` | tenant の最古の待ち（無ければ 0） |
| `tsls_tenant_in_flight` | gauge | `tenant` | tenant の promised + starting + busy |
| `tsls_tenant_max_concurrency` | gauge | `tenant` | tenant quota。無制限なら系列なし |
| `tsls_tenant_grants_total` | counter | `tenant` | tenant への grant（cold / warm）の累計 |
| `tsls_start_rate_tokens` | gauge | | cold start の token の残り |

### 3.4 admission・scale の event

| family | type | labels | 意味 |
|---|---|---|---|
| `tsls_admission_arrivals_total` | counter | | 初回の到着（再キューは数えない。即時拒否も数える） |
| `tsls_admission_grants_total` | counter | `kind` | `cold`（新しい環境を予約）/ `warm`（pool の環境を約束） |
| `tsls_admission_rejections_total` | counter | `reason` | `queue_full` `quota` `capacity` `queue_deadline` `circuit_open` `placement` `function_deleted`（すべて 0 から出す） |
| `tsls_admission_coalesced_waits_total` | counter | | autoscaler の gate（起動中・ready の環境が desired を満たす）で待たされた後に grant された数 |
| `tsls_admission_starts_avoided_total` | counter | | そのうち既存環境（warm）で処理され、起動を 1 回省いた数 |
| `tsls_environment_starts_total` | counter | `result` | breaker に報告された cold start の結果（`success` / `failure`） |
| `tsls_circuit_breaker_opens_total` | counter | | breaker が open になった回数 |
| `tsls_scale_events_total` | counter | `kind`, `reason` | `activation` `scale_up` `prestart` `scale_down` `scale_to_zero` `drain` と理由（`backlog`、`idle_ttl`、`min_ready`、`alias_switch` など）。まだ無い kind は `reason="none"` で 0 |
| `tsls_gate_refusals_total` | counter | `error_type` | invoke gate（設定 cache・認可 lease・control plane 停止時の起動拒否、PLT-4636）が拒否した受付・cold start（`Host.ConfigExpired` など）。無ければ `error_type="none"` で 0 |

### 3.5 attempt・boot identity

| family | type | labels | 意味 |
|---|---|---|---|
| `tsls_attempts_total` | counter | `start_kind`, `status` | 終わった attempt（`cold` / `warm` / `restored`、`succeeded` / `failed` / `outcome_unknown`）。warm dispatch が届かず cold でやり直した分の warm attempt は数えない |
| `tsls_attempt_phase_seconds` | histogram | `phase`, `start_kind` | host が測った時間。`queue_wait` `boot` `init` `resume` `handler` `total`。`boot` と `init` は起動した attempt（cold / restored）だけ（warm の 0 で分布を歪めない）。bucket: 5 ms 〜 300 s |
| `tsls_boot_identity_checks_total` | counter | `result` | §4 |

cold / warm 率は `tsls_attempts_total` から出す: `sum(rate(tsls_attempts_total{start_kind="warm"}[5m])) / sum(rate(tsls_attempts_total[5m]))`。

### 3.6 host 使用量（provider の `environment_stats`）

| family | type | labels | 意味 |
|---|---|---|---|
| `tsls_environment_cpu_seconds_total` | counter | `environment`, `tenant`, `revision`, `state`, `scope` | その環境の host CPU 時間 |
| `tsls_environment_memory_bytes` | gauge | 同上 | 現在の memory |
| `tsls_environment_memory_peak_bytes` | gauge | 同上 | memory の peak |
| `tsls_environment_stats` | gauge | `result` | この scrape で使用量が取れた（`available`）/ 取れなかった（`unavailable`）live 環境の数 |
| `tsls_idle_environment_cpu_seconds_total` | counter | | 2 回続けて idle だった環境の、その間の CPU 時間の累計 |
| `tsls_idle_environment_cpu_ratio_max` | gauge | | 前回と今回の scrape でともに idle だった環境の「CPU 秒 / 経過秒」の最大（無ければ 0） |
| `tsls_idle_environments_sampled` | gauge | | 上の計算に使えた環境の数 |

`scope` が何を測ったかを示す。provider ごとに比べられない:

| provider | scope | 中身 |
|---|---|---|
| firecracker | `cgroup_v2` | VMM の cgroup（`cpu.stat` の `usage_usec`、`memory.current`、`memory.peak`）= guest + VMM。host cgroup が無い構成（`mode = "off"` など）では取れない（`unavailable`）。KVM 実測あり（`docs/evidence/kvm-final-metrics-load-20260917T150242Z/`: pool の全環境で `scope="cgroup_v2"` の系列が出る。起動中でまだ cgroup の無い環境は `unavailable` に数える） |
| process（Linux） | `procfs` | bridge プロセス自身の `/proc/<pid>/stat` utime + stime、`VmRSS` / `VmHWM`。子プロセス（user の handler）は含まない（下限値） |
| process（macOS） | `proc_pid_rusage` | bridge プロセス自身の CPU 時間と phys footprint / lifetime max。子プロセスは含まない |
| fake | `fake` | テストが設定した値 |

process provider は idle の段階が無い（destroy-after-invoke）ので、idle CPU は常に測定対象 0 件（`tsls_idle_environments_sampled 0`）。

### 3.7 dispatcher・設定 cache・非同期 outbox

| family | type | labels | 意味 |
|---|---|---|---|
| `tsls_dispatcher_fenced` | gauge | | lease を失い新しい仕事を取らないとき 1 |
| `tsls_dispatcher_heartbeats_total` | counter | `result` | `renewed` / `fenced` / `error` |
| `tsls_dispatcher_slot_lease_renewals_total` | counter | | heartbeat が更新した slot lease の数 |
| `tsls_config_generation` | gauge | | 適用済みの最大 generation |
| `tsls_config_synced` | gauge | | 一度でも配信を受けたら 1 |
| `tsls_config_consecutive_failures` | gauge | | 最後の成功以降の refresh 失敗数（> 0 は control plane 不達） |
| `tsls_config_valid_remaining_seconds` | gauge | | 設定の有効期限までの秒数（負は期限切れ） |
| `tsls_auth_lease_remaining_seconds` | gauge | | 認可 lease の残り秒数 |
| `tsls_config_reconnects_total` | counter | | 失敗の後に成功した refresh |
| `tsls_config_entries` | gauge | | cache の entry 数 |
| `tsls_async_outbox_pending_events` | gauge | | 未 publish の outbox event（PLT-4639。outbox を持つ gateway だけ） |
| `tsls_async_outbox_oldest_pending_age_seconds` | gauge | | 最古の未 publish event の滞留時間 |
| `tsls_async_outbox_sent_retained_events` | gauge | | publish 済みで保持中の event |
| `tsls_async_queue_condition` | gauge | `condition` | publisher が最後に見た queue の状態（`healthy` / `full` / `unavailable`）が 1 |
| `tsls_usage_journal_healthy` | gauge | | usage journal を読み書きできれば 1（PLT-4642、ADR-0012） |
| `tsls_usage_journal_admitting` | gauge | | journal に admission headroom より多くの余りがあり、新規 invoke を計測付きで受け付けるなら 1。0 の間は 503 `usage_journal_full`（`accept_unmetered` の dev profile を除く） |
| `tsls_usage_journal_pending_events` | gauge | | journal に書いて ledger へまだ運んでいない usage event の数 |
| `tsls_usage_journal_pending_bytes` | gauge | | その bytes |
| `tsls_usage_journal_max_events` | gauge | | `[usage] journal_max_events` |
| `tsls_usage_journal_max_bytes` | gauge | | `[usage] journal_max_bytes` |
| `tsls_usage_unjournaled_events_total` | counter | | journal が拒否した（満杯・停止）usage event の累計。計測なし・推測しない |
| `tsls_usage_collector_runs_total` | counter | | このプロセスの usage collector の実行回数 |
| `tsls_usage_collector_failing` | gauge | | 直前の collector の実行が失敗（ledger 停止、journal の integrity）なら 1 |
| `tsls_usage_collector_last_success_age_seconds` | gauge | | collector が最後に配送に成功してからの秒数（collector の遅れ）。初回成功までは系列なし |
| `tsls_usage_collector_delivered_events_total` | counter | | このプロセスの collector が ledger に渡した journal 行の累計（再配送を含む） |
| `tsls_usage_ledger_events` | gauge | | usage ledger の event 数（event_id で一意） |
| `tsls_usage_ledger_duplicates_ignored_total` | counter | | ledger が既に持っていた event id として捨てた配送の累計 |
| `tsls_metrics_series_truncated` | gauge | `dimension` | cardinality 上限で `_other` に畳んだ数（`revision` / `tenant` / `environment`） |

### 3.8 trigger（PLT-4641）

trigger を持つ gateway（`invokeAsync` が有効な `combined`）だけが出す。label は閉じた集合だけで、tenant・trigger id・event id は付けない（`docs/adr/0014-cron-and-webhook-triggers.md`）。fire が受け付けた invocation 自体の数・実行結果は非同期 invocation と dispatcher の family に出る。

| family | type | labels | 意味 |
|---|---|---|---|
| `tsls_trigger_scheduler_owner` | gauge | | 最後の scheduler の pass でこの gateway が lease を持っていれば 1（同じ `state.db` の gateway のうち 1 つだけが 1） |
| `tsls_trigger_cron_fires_total` | counter | `result` | cron の予定時刻の処理結果: `accepted`（invocation を受け付けた）/ `already_fired`（再起動・別 scheduler で既に発火済み。増え続けるなら二重に回っている）/ `refused`（恒久的な拒否を記録）/ `deferred`（backlog・queue・設定 cache などの一時的な拒否で次の pass に回した）/ `inactive`（無効化・削除・変更との競合） |
| `tsls_trigger_cron_missed_runs_total` | counter | `action` | late（`grace_seconds` を過ぎた）予定時刻を missed-run policy が `run`（実行を試みた）/ `skipped`（policy または `max_catchup_seconds` で捨てた） |
| `tsls_trigger_webhook_deliveries_total` | counter | `result` | webhook の配信結果: `accepted` / `replayed`（event id か署名の dedup で同じ invocation）/ `signature_refused` / `timestamp_refused`（401）/ `too_large`（413）/ `invalid_event_id`（400）/ `disabled`（410）/ `not_found`（404）/ `refused`（受付の拒否: 409 / 429 / 503） |

`signature_refused` と `timestamp_refused` の急増は偽造・再送の試行か sender の時計ずれ、`deferred` の継続は outbox の滞留（`tsls_async_outbox_*`）を示す。

### 3.9 非同期 dispatcher・retry・dead letter（PLT-4640）

dispatcher を動かす gateway（`[queue]` と durable ledger があり `[async_dispatch] enabled = true`）だけが出す。label はすべて固定の文字列で、tenant・function・invocation の id は載せない（dead letter の中身は `GET /v1/functions/{id}/dead-letters` で tenant ごとに読む）。counter はプロセスのメモリにあり再起動で 0 に戻る。

| family | type | labels | 意味 |
|---|---|---|---|
| `tsls_async_dispatch_deliveries_total` | counter | `outcome` | 処理した配送の結果: `completed`（terminal を確定して ACK）、`rescheduled`（次の試行を確定して ACK）、`dead_lettered`、`skipped_terminal`（terminal の再配送。ACK 喪失・重複）、`skipped_stale`（古い generation）、`skipped_claimed`（他の run が claim 中）、`not_due`（NAK）、`poison`（term）、`failed`（台帳の失敗・failpoint。再配送を待つ）、`lost_claim`（settle の fence に負けた） |
| `tsls_async_dispatch_queue_operations_total` | counter | `operation`, `result` | `ack` / `nak` / `term` の成否。ACK は必ず台帳の commit の後 |
| `tsls_async_dispatch_runs_in_flight` | gauge | | この gateway が今処理している配送 |
| `tsls_async_retries_scheduled_total` | counter | `kind` | 次の generation の event を確定した数: `retry`（`max_attempts` に数える）/ `deferral`（容量・retry budget・停止。数えない） |
| `tsls_async_dead_letters_total` | counter | `reason` | この gateway が確定した dead letter（`non_retryable` / `attempts_exhausted` / `expired` / `function_deleted` / `revision_unavailable` / `poison`） |
| `tsls_async_redrives_total` | counter | | dead letter から作った新しい invocation |
| `tsls_async_reaper_actions_total` | counter | `action` | reaper が確定したもの: `abandoned`（claim 切れの run を次の試行へ）、`republished`（broker が失った event）、`dead_lettered`、`lost`、`failed` |

見方: `skipped_terminal` が増えるのは ACK の喪失か重複配送で、実行はしていない。`retries_scheduled_total{kind="deferral"}` の増加は容量不足か retry budget の頭打ち、`dead_letters_total{reason="attempts_exhausted"}` の急増は依存先の障害を示す。alert は定義していない。

### 3.10 予算（PLT-4643、ADR-0016）

金額は価格表の通貨の 10⁻⁶ 単位（`*_micros`）で、PLT-4642 の**仮料金**。tenant label の系列は現在の期間（UTC の暦月）について、確約（reserved + settled + unmetered hold）の大きい順に `[metrics] max_tenant_series` 件まで（それ以上は出さない）。counter はこのプロセスの累計。

| family | type | labels | 意味 |
|---|---|---|---|
| `tsls_budget_enabled` | gauge | | `[budget] enabled` なら 1 |
| `tsls_budget_store_healthy` | gauge | | 予算 store（`usage/budget.db`）を読み書きできれば 1。0 の間は 503 `Host.BudgetStoreUnavailable` |
| `tsls_budget_collector_stalled` | gauge | | finish 済みで未精算の run が `max_unsettled_age_seconds` を越えていれば 1。1 の間は新規 invoke を 503 `Host.BudgetUnknown` |
| `tsls_budget_active_reservations` | gauge | | reserved のままの予約（全 tenant） |
| `tsls_budget_unsettled_runs` | gauge | | 終わったが利用量がまだ精算されていない run |
| `tsls_budget_oldest_unsettled_age_seconds` | gauge | | そのうち最古の run の待ち時間（予算から見た collector の遅れ）。無ければ系列なし |
| `tsls_budget_reserved_micros` | gauge | `tenant` | 未精算の run が予約している最大料金の合計 |
| `tsls_budget_settled_micros` | gauge | `tenant` | 精算済み run の仮料金の合計 |
| `tsls_budget_unmetered_hold_micros` | gauge | `tenant` | 計測しきれなかった run（失効、journal に入らなかった event、unknown の区間）について予約のまま残した額。課金額ではない |
| `tsls_budget_remaining_micros` | gauge | `tenant` | hard limit − 確約（0 で下げ止まり）。hard limit のある tenant だけ |
| `tsls_budget_reservations_total` | counter | | このプロセスが作った予約 |
| `tsls_budget_refusals_total` | counter | `reason`, `cause` | 予算による拒否。`reason`: `budget_exhausted` / `budget_unknown` / `budget_store_unavailable`。`cause`: `tenant_hard_limit` / `function_hard_limit` / `not_delivered` / `lease_expired` / `price_table_mismatch` / `collector_stalled` / `store_unavailable`（すべての組を 0 から出す） |
| `tsls_budget_recheck_refusals_total` | counter | | queue で待った後、grant の時点の再確認で拒否した invocation |
| `tsls_budget_transitions_total` | counter | `result` | 予約の終わり方: `settled` / `settled_incomplete`（unmetered hold が残った）/ `released` / `expired` |
| `tsls_budget_alerts_total` | counter | `scope` | soft limit の閾値を初めて越えた回数（`tenant` / `function`）。alert は何も止めない |
| `tsls_budget_overrun_micros_total` | counter | | 実測が予約を超えた額（全額精算した） |

### 3.11 invocation log（ADR-0018）

台帳が durable（`[store] backend = "sqlite"`）なときだけ出る。tenant label は無い。counter はこのプロセスの累計、保存量の gauge は `logs.db` 全体（同じ `data_dir` の他の gateway の分も含む）。

| family | type | labels | 意味 |
|---|---|---|---|
| `tsls_logs_store_healthy` | gauge | | `logs/logs.db` が開いていて直近の flush が commit できれば 1。0 は degraded（invoke は続き、行は捨てて数える。`/readyz` の `logs.healthy`） |
| `tsls_logs_lines_written_total` | counter | | commit した行（marker を除く） |
| `tsls_logs_bytes_written_total` | counter | | commit した行の bytes |
| `tsls_logs_lines_truncated_total` | counter | | commit した行のうち `max_log_line_bytes` で切ったもの |
| `tsls_logs_marker_lines_total` | counter | | 上限・欠落について書いた `[tachyon] ` marker 行 |
| `tsls_logs_lines_dropped_total` | counter | `reason` | 保存しなかった行: `queue_full`（writer queue の上限）/ `store_unavailable`（`logs.db` が batch を拒否・開けない）/ `invocation_limit` / `attempt_limit` / `unattributed`（invocation に属さない行。API で読めないので保存しない） |
| `tsls_logs_queue_lines` | gauge | | writer queue で待っている行 |
| `tsls_logs_queue_bytes` | gauge | | 同じく bytes |
| `tsls_logs_flush_lag_seconds` | gauge | | 直近に commit した batch の最古の行が queue に入ってから commit までの時間。まだ flush していなければ系列なし |
| `tsls_logs_flush_failures_total` | counter | | `logs.db` が拒否した batch（lock、disk 満杯、開けない） |
| `tsls_logs_stored_bytes` | gauge | | 保存している行の bytes（marker 込み、SQLite の page と index は含まない）。`[logs] max_total_bytes` と比べる値 |
| `tsls_logs_stored_lines` | gauge | | 保存している行（marker 込み） |
| `tsls_logs_retention_deleted_lines_total` | counter | `reason` | 保持処理が消した行: `age`（`retention_seconds`）/ `size`（`max_total_bytes`） |
| `tsls_logs_retention_skipped_non_terminal` | gauge | | 直近の総量上限の処理で、台帳が terminal と答えなかったため残した invocation |

見方: `lines_dropped_total{reason="queue_full"}` の増加と `flush_lag_seconds` の伸びは disk が遅いか log の多い handler、`store_healthy = 0` と `reason="store_unavailable"` は `logs.db` の lock か disk 満杯。`stored_bytes` が `max_total_bytes` を超えたままで `retention_skipped_non_terminal` が正なら、実行中の invocation の log だけで上限を超えている。alert は定義していない。

## 4. boot identity（再利用の証跡）

- guest bridge は Hello で `/proc/sys/kernel/random/boot_id` を報告し、環境の `BootEvidence.guest_boot_id` に残る（PLT-4630）。attempt の API（`attempts[].boot_evidence`）にも出る。
- attempt が終わるたびに、その環境で最初に見た boot id（最大 4096 環境、古い順に忘れる）と比べる:
  - `first_boot`: その環境の最初の attempt。
  - `same_boot`: 同じ環境の後続の attempt が同じ boot id を報告した = 同じ guest で処理した（warm 再利用の証跡）。
  - `boot_changed`: 同じ環境 id で boot id が変わった。**0 のままでなければならない**（ERROR log と alert `TslsBootIdentityChanged`）。
  - `unreported`: boot id が無い（guest kernel の無い process provider）。
- `GET /v1/capacity` の `reuse`（node 全体、tenant の情報は無し）:

```json
"reuse": {
  "provider": "process",
  "mode": "every_invocation_boots",
  "reason": "provider `process` reports idle_quiesce as unsupported",
  "first_boots": 0, "same_boot_reuses": 0, "boot_id_changed": 0, "boot_id_unreported": 36
}
```

- 未対応 profile（process provider、`[pool] enabled = false`、idle capability が `Supported` でない provider）は `mode = "every_invocation_boots"` で「毎回起動」と表示する。負荷ハーネスは invocation を読み戻し、attempt 数と環境数が一致すること（`every_attempt_booted_its_own_environment`）で確かめる。
- 限界: boot id は起動時の Hello でだけ報告される。warm の attempt が比べるのは「その環境の記録にある boot id」で、guest が dispatch のたびに boot id を読み直しているわけではない。guest が再起動すれば bridge の接続が切れて環境は失われる（同じ環境 id で黙って別 guest に dispatch する経路は無い）ので、この比較は台帳・pool の取り違え（別環境の session に dispatch する回帰）を検出するためのもの。handler 自身が boot id を返す形の検査は未実装（Firecracker 実測の際に追加する）。
- warm 再利用の boot identity は fake provider で証明している（`crates/application/tests/scaling.rs::metrics_show_zero_to_cap_to_zero_and_boot_identity_proves_warm_reuse`）。Firecracker（warm pool 有効）でも 5 シナリオで `boot_changed` 0・`same_boot` = warm attempt 数を記録した（`docs/evidence/kvm-final-metrics-load-20260917T150242Z/`、1 host・各 1 回）。

## 5. detector と alert

同じ条件を 2 か所に置く。

- 負荷ハーネス: `apps/load/src/detect.rs`。scenario が記録した `samples.jsonl`（`/metrics` の flatten）に対して実行し、`summary.json` の `detectors` に findings と coverage（見る対象があったか）を書く。Prometheus は不要。
- Prometheus: `deploy/prometheus/alerts.yml`。`apps/load/tests/alerts.rs` が YAML として parse し、式が catalog にある family だけを参照し、4 種の detector すべてに rule があることを確認する（promtool は CI に無い。手元にあれば `promtool check rules deploy/prometheus/alerts.yml`）。

| detector | harness の条件（sample ごと） | alert |
|---|---|---|
| reservation overshoot | reserved CPU / memory / storage > node capacity、`tsls_node_in_flight` > `tsls_node_max_concurrency`、revision の starting + busy + promised > `tsls_revision_max_environments`、tenant の in-flight > quota | `TslsReservedMemoryOverCapacity` `TslsReservedCpuOverCapacity` `TslsInFlightOverMaxConcurrency` `TslsRevisionOverMaxEnvironments` `TslsTenantOverQuota` |
| starvation | tenant の最古の待ち > 5 s（`--starvation-seconds`）で、その待ちが始まった時点の sample から今までに他 tenant の grant が 1 以上増えた（tenant ごとに 1 回報告） | `TslsTenantStarved`（1 分窓で近似） |
| idle 中の資源利用 | `tsls_idle_environments_sampled` > 0 かつ `tsls_idle_environment_cpu_ratio_max` > 0.05（`--idle-cpu-ratio`） | `TslsIdleEnvironmentCpu`、`TslsIdleCpuAccumulating` |
| boot identity | `tsls_boot_identity_checks_total{result="boot_changed"}` の増加 | `TslsBootIdentityChanged` |

ほかに `TslsCircuitBreakerOpen`、`TslsDispatcherFenced`、`TslsConfigurationExpired`。閾値は prototype の出発点で SLO ではない。

gateway が応答しない sample（再起動中）は detector が無視する。gateway の再起動で counter は 0 に戻る（boot identity の比較は再起動の前後を跨がない）。

## 6. 負荷シナリオ（`scripts/load/scenarios.sh`）

```sh
scripts/load/scenarios.sh                       # 全シナリオ、process provider
scripts/load/scenarios.sh lifecycle             # 受入のグラフ
TSLS_LOAD_SEED=42 scripts/load/scenarios.sh burst mixed
```

| シナリオ | 内容 | 期待（`summary.json` の `checks`） |
|---|---|---|
| `single` | 0.2 s の単発を 2 s 間隔で 3 回 | zero_before, rose, zero_after, all_succeeded, no_findings, reuse mode |
| `burst` | 1 → 2 → 8 並列 × 24 件（revision の cap 3 を超える）→ 1 → 0 まで idle | + cap(3), queue, decreased |
| `idle-to-zero` | burst → 0 まで idle → 5 s idle のまま → 再アクセス → 0 | zero_after ほか |
| `mixed` | tenant A: 2 s × 12 件を 6 並列、tenant B: 0.1 s × 16 件を 2 並列、同時（node 4、tenant quota 3） | two_tenants_served, cap(4), no_findings（starvation detector） |
| `restart` | 負荷 → 0 → idle 中に gateway を SIGTERM・再起動 → 負荷 → 0 | restart, active_after_restart, zero_after_restart |
| `lifecycle` | 0 → 1 → 2 → 8 並列（cap 3）→ 1 → 0 → idle → 再起動 → 2 並列 → 0 | 上のすべて。**受入の 0→増→上限→減→0→再起動のグラフ** |

- **宣言した上限**: `TSLS_LOAD_MAX_CONCURRENCY`（既定 12）、`TSLS_LOAD_MAX_REQUESTS`（既定 150、シナリオ全体）、`TSLS_LOAD_MAX_DURATION_SECONDS`（既定 240）。`tsls-load` は計画がこれを超えると 1 件も送らずに exit 2、実行中は期限の 0.5 s 前から送らない。上限そのものにも compile 時の天井がある（64 並列・2000 件・1800 s）。`summary.json` の `limits_observed` が実際の最大同時数・件数・最終応答時刻と `respected` を記録する（上限超過は `ok = false`）。
- **送り先の制限**: `127.0.0.1`・`::1`・`localhost` だけ。ほかは `--lab-host`（`TSLS_LOAD_LAB_HOST`）で明示した host だけで、名前解決はしない。redirect は追わない。本番・外部 host には送らない（`limits::tests::only_loopback_or_an_explicit_lab_host_is_a_load_target`）。
- **再現性の記録**: `run.json` に commit・tracked file の未 commit 変更数・provider・seed・sample 間隔・上限・gateway 設定（token は伏せる）とその SHA-256・host（`uname`）・rustc・revision の設定。jitter は seed から決まる（splitmix64 + xorshift64*）。
- **出力**（`docs/evidence/load-<scenario>-<UTC>-<provider>/`）: `run.json`、`gateway.toml`、`samples.jsonl`（`t_ms`・`up`・`/metrics` の counter / gauge・tenant A の `/v1/capacity` の要約）、`requests.jsonl`、`invocations.jsonl`（start kind・環境・boot id）、`phases.jsonl`、`summary.json`（requests の p50 / p95 / max・lifecycle・reuse・detectors・checks・`ok`）、`timeline.svg`（環境の状態と queue の折れ線、phase の帯、gateway 停止の灰色帯）、`timeline.txt`（端末用）、`report.txt`、`gateway.log`。
- **provider 非依存**: `TSLS_GATEWAY_CONFIG` と `TSLS_API_URL`・`TSLS_TOKEN_A`・`TSLS_TOKEN_B`・`TSLS_METRICS_TOKEN`・`TSLS_GUEST_DIR`・`TSLS_PROVIDER` を渡せば任意の provider の gateway で同じシナリオを回せる。reuse の期待は gateway の報告（`reuse.mode`）で決まり、warm pool 有効なら `warm_reuse`（boot id が変わらず、2 回以上使われた環境がある）を確認する。Firecracker では burst・idle-to-zero・mixed・restart・lifecycle を 1 回ずつ実行し 5/5 ok（`docs/evidence/kvm-final-metrics-load-20260917T150242Z/`: profile production・jailer / cgroup required・`[pool]` 有効。休止中の microVM の idle CPU ratio は常に 0、findings 0）。provider の workdir は jailer の `chroot_base`（既定 `/srv/jailer`）と同じ file system に置く（`/tmp` が別 file system の host では readiness が 503 のまま）。
- `scripts/e2e/` ではなく `scripts/load/` に置く（`scripts/e2e/*` の変更は KVM gate を要求する。`docs/ci.md` §3）。helper は `scripts/e2e/lib.sh` を読み込むだけで変更しない。
