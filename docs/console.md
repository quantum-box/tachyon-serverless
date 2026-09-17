# Functions console（PLT-4644、最小）

CLI 以外からプロトタイプの状態と失敗原因を確認するための最小画面。`apps/console` にある。**正本は管理 API**（`docs/api.md`、`docs/openapi.json`）で、console は公開 API の client にすぎない。新しい backend 権限・内部 API・独自のデザインシステムは足していない。

- 状態: プロトタイプ。**既存 Tachyon Console（quantum-box/tachyon-apps）への統合は未着手（設計のみ、`docs/console-integration.md`）**。
- UI 無しでも P1 デモは完了する（`scripts/e2e/demo.sh` は console を使わない）。

## 1. 構成

| 項目 | 内容 |
|---|---|
| framework | Next.js 14.2.35 App Router（tachyon-apps `apps/tachyon` と同じ版）、React 19.2.6、TypeScript 5.8.3 |
| 出力 | `output: 'export'` の静的ファイル（`apps/console/out/`）。Next.js server・API route・server action・middleware は無い |
| UI | shadcn/ui new-york（`src/components/ui/*`、tachyon-apps `apps/tachyon/src/components/ui/*` と同じ実装）+ `@tachyon-sdk/native-ui` の Tailwind v3 preset と `tokens.css`（`github:quantum-box/native-ui#2d44249`、tachyon-apps と同じ commit）。console 独自の色・token は無い（状態 badge も preset の `success` / `warning` / `destructive` だけ） |
| data | SWR 2.4.2（tachyon-apps と同じ）、`fetch` の薄い client（`src/lib/serverless-api/client.ts`） |
| API 型 | `docs/openapi.json` から `openapi-typescript` で生成（`src/gen/openapi/serverless-api.ts`、`pnpm gen:api`）。`pnpm check:api` が古い生成物を検出 |
| toolchain | node 22.21.1 / pnpm 10.34.5（`apps/console/mise.toml`、`package.json` の `engines` / `packageManager`、`pnpm-lock.yaml`）。`node_modules` と `out/` は git に入れない |
| lint / test | Biome 1.9.4、`tsc --noEmit`、vitest 3.2.7、Playwright 1.61.1（Chromium headless） |

## 2. 配信方式（選択）

**gateway が `/console/` で静的ファイルを配る**方式を正とした。console と API が同一 origin になり、CORS も token を別 host に送る設定も要らない。`next dev` の proxy は開発用。

```toml
# gateway 設定（既定は無効）
[console]
enabled = true
dir = "apps/console/out"   # 省略時も同じ。相対 path は gateway の作業 directory 基準
```

- `[console]` は gateway（`apps/gateway/src/console.rs`）が同じ設定ファイルから読む。`GatewayConfig`（`crates/application`）は変えていない。未知の key は拒否する。
- `enabled = true` で `dir/index.html` が無ければ gateway は起動しない。
- `GET /console` → `308 /console/`。`/console/*` は `ServeDir`（`..` を含む path は root の外に出ない）、未知の page は export の `404.html` を 404 で返す。POST 等は 405。
- console の応答には `Content-Security-Policy`（`default-src 'self'`、`connect-src 'self'`、`frame-ancestors 'none'` など。Next.js の inline bootstrap のため `script-src 'unsafe-inline'`）、`X-Frame-Options: DENY`、`X-Content-Type-Options: nosniff`、`Referrer-Policy: no-referrer`、HTML は `Cache-Control: no-store`（`_next/static` は immutable）。
- console の route は `/v1` の外で、credential を持たない。page から API への呼び出しは閲覧者の token で `/v1` の認証・tenant 検査をそのまま通る（`apps/gateway/tests/console_static.rs`）。
- `profile = "production"` でも有効にできる（静的ファイルだけで権限は増えない）。外部公開する構成では `/metrics` と同じく reverse proxy 側で経路を制御すること。

## 3. 実行

```bash
cd apps/console
pnpm install --frozen-lockfile
pnpm build                       # -> out/
# gateway 設定に [console] enabled = true を足して起動し、http://127.0.0.1:8080/console/ を開く

pnpm dev                         # http://127.0.0.1:3100/console/、/v1/* を TSLS_API_URL（既定 http://127.0.0.1:8080）へ proxy
pnpm lint && pnpm ts && pnpm test && pnpm check:api   # CI の console job と同じ
```

mise を使う場合は `apps/console` で `mise exec -- pnpm ...`（`mise.toml` の node / pnpm）。

### E2E（Playwright）

```bash
scripts/console/e2e.sh [--evidence DIR] [-- <playwright args>]
```

cargo build（gateway・tsls・bridge・`example-hello`・`example-cpu-burn`）→ console の install / build → 使い捨ての gateway（process provider、scratch `data_dir`、dev 専用 SQLite queue、`[console] enabled`、tenant A の `deploy+invoke+redrive` token・`operator` token、tenant B の token、実行ごとに乱数の token と demo secret 値、`[budget]` はファイル配信で両 tenant に大きな hard limit・tenant A に alert が必ず発火する soft limit）→ 公開 API で seed（hello v1 / v2、OCI 参照で `failed` になる revision、cpu-burn、失敗し続けて dead letter になる非同期 invocation、tenant B の function と invocation）→ `apps/console/e2e/console.spec.ts`（15 シナリオ、直列）。証跡（既定 `target/console-e2e/console-<UTC>/`）: `summary.txt`、`playwright.txt`、`results.json`、`playwright-report/`、`screenshots/`、`seed-ids.json`（id だけ）、`gateway.log`。終了時に証跡 directory 全体を token と secret 値で grep し、見つかれば失敗にする。Playwright の trace は bearer token を記録するので出力しない。

CI では Playwright を回さない（gateway の build・Chromium・実 invoke で hosted runner の 10 分に収まる保証が無い）。静的検査は CI の `console` job（`docs/ci.md`）。

**mock を使うシナリオ**（`page.route` で API 応答を差し替え。実 gateway では決定的に作れない状態だけ）:

| 状態 | 理由 |
|---|---|
| `outcome_unknown`（invocation 詳細と test invoke の 502） | process provider で「Invoke 送信後の bridge 切断」を任意に起こす手段が無い。応答の形は `docs/api.md` §4・§5.8 のもの |
| budget API が無い gateway（404 `no route`） | この repo の main には PLT-4643 が入っているので、古い gateway の応答を再現するため |
| loading / empty / error（500）/ unauthorized（401） | 遅延・空の tenant・store 障害・revoke を実 gateway で順に作るのは不安定 |

それ以外（403、他 tenant の 404、budget の状態と alert、rollback / cancel / redrive、retry と redrive の区別）は実 gateway の応答。

## 4. 画面

| 画面 | path（`/console` 配下） | API |
|---|---|---|
| sign-in | `/` | `GET /v1/functions`（token と tenant id の確認） |
| Function 一覧 | `/functions/` | `GET /v1/functions` |
| Function 詳細: Deployments | `/functions/detail/?id=` | revisions（未確定の間 1.5 s poll、`failed` は理由）、aliases、rollback（確認 dialog → `PUT aliases/{alias}` を `expected_generation` 付きで → toast） |
| Test invoke | `…&tab=invoke` | `POST …/invoke` / `…/invokeAsync`（alias / 固定 revision、JSON 入力、結果と invocation へのリンク） |
| Invocations | `…&tab=invocations` | `GET …/invocations?limit=`（status / mode / origin / revision で絞り込み、非 terminal があれば poll）、origin 判定に dead letter（下の §6） |
| Dead letters | `…&tab=dead-letters` | `GET …/dead-letters` |
| Usage（function） | `…&tab=usage` | `GET …/usage`（事実だけ、`not_billable`） |
| Invocation 詳細 | `/invocations/detail/?id=[&attempt=]` | 詳細（attempts・timings・boot evidence・deadlines・dispatch・redrive 元）、logs（attempt / stream で絞り込み）、cancel（確認 → `POST …/cancel` → toast） |
| Dead letter 詳細 | `/dead-letters/detail/?id=` | 詳細、redrive 記録、redrive（確認 dialog・理由 → `POST …/redrive` → toast と新 invocation へのリンク） |
| Usage | `/usage/` | `GET /v1/usage`（期間・group_by・function）。**「Provisional estimate — not an invoice」banner を常に表示**、API の `notice` も表示 |
| Budget | `/budget/` | `GET /v1/budget?period=`（PLT-4643、`BudgetReportResponse`）。provisional banner、admitting / refusing と理由、hard limit・committed・remaining・reserved・settled・unmetered hold・overrun、soft limit と発火した alert、function ごとの予算、`guarantee`。`[budget] enabled = false` の gateway ではその旨、route が無い古い gateway では「Not available on this gateway」 |
| Capacity | `/capacity/` | `GET /v1/capacity`（5 s poll） |

どの data view も loading（skeleton）/ empty / error（retry）/ permission denied（403）/ not found（404、他 tenant を含む）/ unauthorized（401、再 sign-in）/ unavailable（route 不在・`async_unavailable`）を出す（`src/components/functions/data-state.tsx`）。

## 5. 秘密情報と tenant 境界

- token は React state と**この tab の `sessionStorage`** だけ（`localStorage`・cookie・URL・console log に出さない）。sign-out で消し、SWR cache も捨てる。cache key は token そのものではなく fingerprint。
- API client は `/v1/` で始まる同一 origin の path しか呼ばない（`buildUrl`）。`credentials: 'omit'`、`redirect: 'error'`。
- secret の値は API に存在しない（binding 名だけ）。revision の env var の値は既定で伏せ、明示操作で表示。
- invocation の入力本文は API が返さない（digest と size だけ）。「Reveal input」は API に取得手段が無いことを説明する（再取得できる endpoint は無い）。test invoke の入力は component state だけで、保存・URL・履歴に残さない。invocation の出力も既定で折りたたみ。
- **handler 自身が書いたログは API が返すとおりに表示する**（例: `example-hello` は stderr に payload を出す）。ログに secret や入力を書かないのは handler の責任。
- tenant 境界は API が強制する。console は他 tenant の id を URL で渡されても API の 404 を表示するだけ（E2E `cross-tenant URLs …`）。gateway 側の検査に穴は見つからなかったので gateway の test は足していない。

## 6. retry と新規実行の区別

- **retry** = 同じ invocation の attempt 2 以降（詳細の Attempts に `retry` badge、一覧の attempts 列に「(n retries)」）。
- **新規実行** = test invoke をもう一度押す、または redrive。別の invocation id。redrive で作られた invocation は詳細に「redrive (new invocation)」と元 invocation・dead letter・理由・依頼者。
- API の制約: `GET /v1/functions/{id}/invocations` の item には `dispatch` が無く、`GET /v1/functions/{id}/dead-letters` の item の `redrives` は常に空。一覧で redrive 由来を示すため、`redrive_count > 0` の dead letter を最大 50 件まで詳細取得して対応付けている（`invoke` role が無い、または非同期が無効な gateway では一覧の origin は `new` と表示され、正しい origin は詳細 page だけ）。

## 7. 制約・未実装

- UI 文言は英語のみ（tachyon-apps の en/ja 辞書への移植は統合時）。
- id は query parameter（static export は id ごとの page を作れないため）。統合時は path segment にする。
- 一覧の paging は `limit`（最大 500）だけ（API に cursor が無い）。フィルタは取得済みの範囲に対してだけ効く。
- Function の作成・deploy・削除、trigger の管理は画面に無い（CLI）。
- budget の設定変更（limit の編集）は画面に無い（control plane の設定ファイル）。
- 認証は静的 token（`[[identity.tokens]]`）の貼り付け。Tachyon のユーザー session との連携は無い（`docs/console-integration.md`）。
- Playwright E2E は macOS arm64 の手元実行のみ（CI に無い）。
