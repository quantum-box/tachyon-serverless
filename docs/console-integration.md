# 既存 Tachyon Console への統合計画（PLT-4644、設計のみ・未着手）

`apps/console`（この repo）を quantum-box/tachyon-apps の Tachyon Console に移すための設計。**tachyon-apps への PR は出していない**。調査は 2026-09-17 時点の tachyon-apps `main` の shallow clone を読んだだけ（変更なし）。

## 1. 調査結果（tachyon-apps）

| 項目 | 内容 | 場所 |
|---|---|---|
| app | `apps/tachyon`（package `tachyon`、private） | `apps/tachyon/package.json` |
| framework | Next.js 14 App Router（lock 14.2.35）。本番は vinext 0.0.53（Vite 8.1.4 + `@vitejs/plugin-rsc`）で Cloudflare Workers に deploy。route の `params` は `Promise` を await する形 | `apps/tachyon/vite.config.ts`、`wrangler.jsonc` |
| React / TS | React 19.2.6（root の `pnpm.overrides`）、TypeScript 5.8.3 | root `package.json` |
| monorepo | pnpm 10.34.5 + Turborepo 2.5、node 20（volta 20.11.1）、Biome 1.9 | root `package.json`、`biome.json` |
| UI | local の shadcn/ui new-york（`src/components/ui/*`、`components.json`: style new-york、baseColor slate、cssVariables、lucide）。Tailwind 3.4.19 + `@tachyon-sdk/native-ui` preset（`github:quantum-box/native-ui#2d44249`、public repo、npm 未公開、source を transpile）。`packages/ui` は旧来の React 18 実装で未使用 | `apps/tachyon/tailwind.config.ts`、`src/app/layout.tsx`（`tokens.css` の import） |
| toast | Radix toast（`@/components/ui/toaster` を root layout に置く、86 files）。sonner / react-hot-toast も一部 | `src/app/layout.tsx` |
| data | SWR 2.4.2、GraphQL codegen（`src/gen/graphql*.ts`）、REST は `fetch` wrapper（`src/lib/apps-api/*`、型は `src/gen/openapi/tachyon-api-types`） | `src/lib/apps-api/client.ts` |
| 認証 | next-auth 5.0.0-beta.31 + Cognito（hosted UI / credentials）+ Google。server は `authWithCheck()` と `session.accessToken`、client は `useSession()` | `src/app/auth.ts`、`src/app/cognito.ts` |
| tenant | URL の `/v1beta/[tenant_id]/...`。backend へは `x-operator-id`（と `x-user-id`）。layout が `sdk.Me()` と `decideTenantRouteAccess` で検査し `TenantProvider` を置く | `src/app/v1beta/[tenant_id]/layout.tsx`、`src/app/providers/TenantProvider.tsx`、`src/lib/tenant-routing-*.ts` |
| browser → backend | `src/app/api/proxy/[...path]/route.ts` の proxy | 同左 |
| product page の例 | Cloud Apps: `v1beta/[tenant_id]/cloud/apps/page.tsx`（server、辞書と `V1BetaSidebarHeader`）→ `apps-client.tsx`、`apps/[app_id]/page.tsx` → `app-detail-client.tsx` と `*-tab.tsx` | `src/app/v1beta/[tenant_id]/cloud/` |
| sidebar | `SidebarItemConfig`（key、title、url、lucide icon、`requiredActions`、`allowedTenantIds`）。Cloud 群は 425 行付近（`ITEM_APP`、`ITEM_STORAGE`） | `src/app/v1beta/[tenant_id]/sidebar-config.ts`、`sidebar-config.test.ts` |
| i18n | TypeScript 辞書（en / ja、既定 en）。`cloud:` は 4021 行付近 | `src/lib/i18n/v1beta-translations.ts`、`src/app/i18n/get-dictionary.ts` |
| feature flag | OpenFeature（`src/lib/feature-flags/keys.ts`、`use-feature-flag.ts`）、開発 tenant 限定（`src/lib/development-features` の `INTERNAL_DEVELOPMENT_TENANT_IDS`） | 同左 |
| test | Playwright（`apps/tachyon/playwright.config.ts`、`src/e2e-tests/`、`auth.setup.ts` で sign-in して storageState）、Vitest、Storybook 8.6 | 同左 |
| 既存の serverless 画面 | 無い。Lambda は Cloud Apps の `deployment_target === 'lambda'` の一部 | `cloud/apps/[app_id]/compute-resources-tab.tsx` など |

この repo の console はこれに合わせた: Next.js 14.2.35 App Router、React 19.2.6、shadcn new-york の同じ component 実装、native-ui の同じ commit の preset と `tokens.css`、SWR、Radix toast、Biome の同じ規則、Playwright。**差分**は §3。

## 2. 置き場所（予定）

`apps/tachyon/src/app/v1beta/[tenant_id]/serverless/` を新設する（Cloud Apps と同じ page / client の分割）。

| この repo | tachyon-apps（予定） |
|---|---|
| `apps/console/src/app/(console)/functions/page.tsx` | `…/serverless/functions/page.tsx`（server: 認証・辞書・`V1BetaSidebarHeader`）+ `functions-client.tsx` |
| `…/functions/detail/page.tsx` + `components/functions/function-detail.tsx` | `…/serverless/functions/[function_id]/page.tsx` + `function-detail-client.tsx` |
| `components/functions/{revisions,invoke,invocations,dead-letters,function-usage}-panel.tsx` | `…/serverless/functions/[function_id]/{deployments,invoke,invocations,dead-letters,usage}-tab.tsx` |
| `…/invocations/detail/page.tsx` + `invocation-detail.tsx`、`invocation-logs.tsx` | `…/serverless/invocations/[invocation_id]/page.tsx` + `invocation-detail-client.tsx`、`logs-panel.tsx` |
| `…/dead-letters/detail/page.tsx` + `dead-letter-detail.tsx` | `…/serverless/dead-letters/[dead_letter_id]/page.tsx` + client |
| `…/usage/page.tsx`、`budget/page.tsx`、`capacity/page.tsx` | `…/serverless/{usage,budget,capacity}/page.tsx` |
| `components/functions/{data-state,status-badge,notices,confirm-action,kv}.tsx` | `…/serverless/_components/`（Cloud Apps に同種があれば置き換え） |
| `components/ui/*` | 追加なし（tachyon-apps の既存 `src/components/ui/*` をそのまま使う） |
| `lib/serverless-api/{client,types,hooks}.ts` | `src/lib/serverless-api/`（`src/lib/apps-api/client.ts` の `buildHeaders` の形に合わせる） |
| `gen/openapi/serverless-api.ts` | `src/gen/openapi/serverless-api-types`（tachyon-serverless の `docs/openapi.json` から生成。codegen の入力に追加） |
| `lib/invocation-kind.ts`、`format.ts` | `src/lib/serverless/` |
| 画面文言 | `src/lib/i18n/v1beta-translations.ts` に `serverless:` slice（en / ja） |
| sidebar | `sidebar-config.ts` の Cloud 群に `ITEM_FUNCTIONS`（`/serverless/functions`、lucide `FunctionSquare`）、`sidebar-config.test.ts` を更新。当面 `allowedTenantIds` / feature flag `serverless-functions-console`（`src/lib/feature-flags/keys.ts`）で開発 tenant に限定 |
| `apps/console/e2e/console.spec.ts` | `apps/tachyon/src/e2e-tests/serverless-functions.spec.ts`（`auth.setup.ts` の storageState を使う。gateway は seed 済みの検証環境を指す） |

## 3. 必要な adapter

### 3.1 認証（最大の未解決点）

この repo の console は gateway の静的 token（`[[identity.tokens]]`）を貼る。Tachyon Console では利用者は next-auth（Cognito）で sign-in しているので、**serverless gateway が Tachyon の利用者を principal として受け付ける仕組みが要る**。候補:

1. **Tachyon backend の proxy（推奨）**: browser → `src/app/api/proxy/[...path]/route.ts` 相当の route（例 `/api/serverless/[...path]`）→ tachyon-api が `session.accessToken` と `x-operator-id` を検証し、tenant / roles を解決して gateway に短命の credential で転送。gateway は既存の `IdentityProvider` port に「Tachyon の署名付き assertion を検証する実装」を足す（gateway 側の変更、この PR の範囲外）。token は browser に出ない。
2. **gateway が Cognito の JWT を直接検証**: `IdentityProvider` に JWKS 検証を実装し、`x-operator-id` → `tenant_id`、Tachyon の policy action → `deploy` / `invoke` / `redrive` / `operator` を対応付ける。browser から gateway へ直接 CORS 呼び出しになり、CORS 設定と gateway の公開が要る。

どちらも「tenant 境界は API 側で強制」は変わらない（console は URL の `[tenant_id]` を `x-tachyon-tenant-id` として送るだけで、判定は gateway）。`TenantProvider` の tenant id と gateway の `tenant_id`（`tn_…`）の対応表が要る（現状 Tachyon の operator id と serverless の tenant id は別の体系）。

### 3.2 置き換える部分

| この repo | Tachyon Console |
|---|---|
| `lib/session.tsx`（sessionStorage の token、sign-in page） | 削除。`useSession()` / `authWithCheck()` と `TenantProvider` |
| `lib/serverless-api/client.ts` の `Authorization: Bearer <tenant token>` | proxy route への同一 origin 呼び出し（§3.1 の 1）または `session.accessToken`（2）。`buildUrl` の「`/v1/` だけ」の制限は proxy prefix に合わせて維持 |
| `lib/navigation.ts` の query parameter の id | `[function_id]` などの path segment（`params` は await）。`routes.*` を `/v1beta/${tenantId}/serverless/...` に |
| `AppShell` | 削除（`V1BetaSidebarHeader` と sidebar） |
| 英語の固定文言 | 辞書 |
| `next.config.mjs` の static export / dev proxy | 不要（tachyon の build に乗る） |

### 3.3 そのまま移せる部分

`data-state.tsx`（loading / empty / error / 403 / 404 / 401 / unavailable）、`status-badge.tsx`（native-ui preset の token だけ）、`notices.tsx`（OutcomeUnknown の説明、provisional banner）、`confirm-action.tsx`（AlertDialog）、`invocation-kind.ts`（retry と redrive の区別）、各 panel の表示ロジック、`lib.test.ts`（Vitest）。

## 4. 統合前に決めること

- §3.1 の方式と、Tachyon の operator / policy と serverless の tenant / role の対応。
- API 側の欠落（`docs/console.md` §6）: history の item に `dispatch`（少なくとも `redriven_from`）、dead-letter 一覧の `redrives`、history の cursor。統合前に API へ足すと一覧の追加取得が不要になる。
- serverless の gateway を Tachyon Console の環境から到達可能にする配置（現状は単一 node の検証環境）。
