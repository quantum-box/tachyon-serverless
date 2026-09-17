import { expect, test } from '@playwright/test'
import { expectNotInDom, seed, shot, signIn } from './fixtures'

const s = seed()
const secrets = () => [s.secretValue, s.tokens.a, s.tokens.b, s.tokens.operator]

test.describe.configure({ mode: 'serial' })

test('sign-in refuses an unknown token and a mismatched tenant, then accepts tenant A', async ({
	page,
}) => {
	await page.goto('/console/functions/')
	// Not signed in: the shell sends the viewer to sign-in and back afterwards.
	await expect(page).toHaveURL(/\/console\/\?next=/)
	await page.getByLabel('API token').fill('not-a-token')
	await page.getByRole('button', { name: 'Sign in' }).click()
	await expect(page.getByTestId('sign-in-error')).toContainText('HTTP 401')

	await page.getByLabel('API token').fill(s.tokens.a)
	await page.getByLabel('Tenant id (optional)').fill(s.tenants.b)
	await page.getByRole('button', { name: 'Sign in' }).click()
	await expect(page.getByTestId('sign-in-error')).toContainText('HTTP 403')
	await shot(page, '01-sign-in-refused')

	await page.getByLabel('API token').fill(s.tokens.a)
	await page.getByLabel('Tenant id (optional)').fill(s.tenants.a)
	await page.getByRole('button', { name: 'Sign in' }).click()
	await expect(page).toHaveURL(/\/console\/functions\/$/)
	await expect(page.getByTestId('session-tenant')).toContainText(s.tenants.a)
	await expectNotInDom(page, secrets())
})

test('function list shows only the tenant’s functions and navigates to the detail', async ({
	page,
}) => {
	await signIn(page, s.tokens.a)
	const rows = page.getByTestId('function-row')
	await expect(rows).toHaveCount(2)
	await expect(page.getByTestId('functions-table')).toContainText('hello')
	await expect(page.getByTestId('functions-table')).toContainText('cpu-burn')
	await expect(page.getByTestId('functions-table')).not.toContainText(
		'tenant-b-private',
	)
	await shot(page, '02-functions')

	await page.getByRole('link', { name: 'hello' }).click()
	await expect(page).toHaveURL(
		new RegExp(`/console/functions/detail/\\?id=${s.ids.hello}`),
	)
	await expect(page.getByTestId('function-name')).toHaveText('hello')
})

test('deploy state: ready and failed revisions, aliases, secrets by binding name only', async ({
	page,
}) => {
	await signIn(page, s.tokens.a)
	await page.goto(`/console/functions/detail/?id=${s.ids.hello}`)
	const cards = page.getByTestId('revision-card')
	await expect(cards).toHaveCount(3)
	const failed = page.locator(`[data-revision-id="${s.ids.revFailed}"]`)
	await expect(failed.getByTestId('revision-status')).toHaveAttribute(
		'data-status',
		'failed',
	)
	await expect(failed.getByTestId('revision-failure')).not.toBeEmpty()
	const v2 = page.locator(`[data-revision-id="${s.ids.revV2}"]`)
	await expect(v2.getByTestId('revision-status')).toHaveAttribute(
		'data-status',
		'ready',
	)
	await expect(v2.getByTestId('revision-alias')).toHaveText('prod')
	await expect(v2.getByTestId('revision-secrets')).toContainText(
		'DEMO_SECRET ← binding demo-secret',
	)
	// Environment values are masked until asked for.
	await expect(v2.getByTestId('revision-env')).toHaveText('GREETING=••••')
	await v2.getByRole('button', { name: 'Show values' }).click()
	await expect(v2.getByTestId('revision-env')).toHaveText('GREETING=v2')
	await shot(page, '03-deployments')
	await expectNotInDom(page, secrets())
})

test('test invoke: synchronous success opens the invocation, its attempt, logs and revision', async ({
	page,
}) => {
	await signIn(page, s.tokens.a)
	await page.goto(`/console/functions/detail/?id=${s.ids.hello}&tab=invoke`)
	await page.getByTestId('invoke-input').fill('{"name":"playwright"}')
	await page.getByTestId('invoke-submit').click()
	await expect(page.getByTestId('invoke-result-ok')).toBeVisible()
	await expect(page.getByTestId('invoke-output')).toContainText(
		'hello, playwright',
	)
	await expect(page.getByTestId('invoke-output')).toContainText(
		'"secret_present": true',
	)
	await expect(page.getByTestId('toast-success')).toContainText(
		'Invocation succeeded',
	)
	await shot(page, '04-invoke-success')
	await expectNotInDom(page, secrets())

	await page.getByTestId('invoke-result-link').click()
	await expect(page).toHaveURL(/\/console\/invocations\/detail\/\?id=inv_/)
	await expect(page.getByTestId('invocation-status')).toHaveAttribute(
		'data-status',
		'succeeded',
	)
	await expect(page.getByTestId('origin-new')).toBeVisible()
	await expect(page.getByTestId('attempt')).toHaveCount(1)
	await expect(page.getByTestId('attempt-kind')).toHaveText('initial')
	// Input: digest and size only; reveal explains the API does not keep the body.
	await expect(page.getByTestId('input-digest')).toContainText('sha256:')
	await page.getByTestId('reveal-input').click()
	await expect(page.getByTestId('input-not-retained')).toBeVisible()
	await expect(page.getByTestId('invocation-output')).toHaveCount(0)
	await page.getByTestId('toggle-output').click()
	await expect(page.getByTestId('invocation-output')).toContainText(
		'hello, playwright',
	)
	await page.getByTestId('toggle-boot-evidence').click()
	await expect(page.getByTestId('boot-evidence')).toContainText('host_pid')
	await expect(page.getByTestId('log-line').first()).toBeVisible()
	await shot(page, '05-invocation-detail')

	// attempt -> its logs
	await page.getByTestId('attempt-logs-link').click()
	await expect(page).toHaveURL(/&attempt=att_/)
	const attemptId = new URL(page.url()).searchParams.get('attempt')
	const lines = page.getByTestId('log-line')
	await expect(lines.first()).toBeVisible()
	for (const attr of await lines.evaluateAll(els =>
		els.map(e => e.getAttribute('data-attempt-id')),
	)) {
		expect(attr === '' || attr === attemptId).toBe(true)
	}
	// invocation -> revision
	await page.getByTestId('invocation-revision-link').click()
	await expect(page).toHaveURL(
		new RegExp(
			`/console/functions/detail/\\?id=${s.ids.hello}#revision-${s.ids.revV2}`,
		),
	)
	await expect(page.locator(`#revision-${s.ids.revV2}`)).toBeVisible()
})

test('test invoke: handler error and asynchronous accept are shown with their invocation', async ({
	page,
}) => {
	await signIn(page, s.tokens.a)
	await page.goto(`/console/functions/detail/?id=${s.ids.hello}&tab=invoke`)
	await page.getByTestId('invoke-input').fill('{"fail":true}')
	await page.getByTestId('invoke-submit').click()
	await expect(page.getByTestId('invoke-result-error')).toBeVisible()
	await expect(page.getByTestId('invoke-error-code')).toHaveText('user_error')
	await expect(page.getByTestId('toast-failure')).toContainText('HTTP 502')
	await shot(page, '06-invoke-error')
	await page.getByTestId('invoke-result-link').click()
	await expect(page.getByTestId('invocation-status')).toHaveAttribute(
		'data-status',
		'failed',
	)
	await expect(page.getByTestId('invocation-error')).toContainText(
		'Demo.Failure',
	)

	await page.goto(`/console/functions/detail/?id=${s.ids.hello}&tab=invoke`)
	await page.getByTestId('invoke-input').fill('{"name":"async"}')
	await page.getByTestId('invoke-mode-async').check()
	await page.getByTestId('invoke-submit').click()
	await expect(page.getByTestId('invoke-result-async')).toBeVisible()
	await page.getByTestId('invoke-result-link').click()
	await expect(page.getByTestId('invocation-status')).toHaveAttribute(
		'data-status',
		'succeeded',
		{ timeout: 60_000 },
	)
	await expect(page.getByTestId('dispatch')).toBeVisible()
	// Invalid JSON never reaches the API.
	await page.goto(`/console/functions/detail/?id=${s.ids.hello}&tab=invoke`)
	await page.getByTestId('invoke-input').fill('{not json')
	await page.getByTestId('invoke-submit').click()
	await expect(page.getByText('Input is not valid JSON')).toBeVisible()
})

test('invocation history: filters, retries versus new invocations, and dead letters', async ({
	page,
}) => {
	await signIn(page, s.tokens.a)
	await page.goto(
		`/console/functions/detail/?id=${s.ids.hello}&tab=invocations`,
	)
	const dlqRow = page.locator(
		`[data-testid="invocation-row"][data-invocation-id="${s.ids.dlqInvocation}"]`,
	)
	await expect(dlqRow).toBeVisible()
	await expect(dlqRow).toHaveAttribute('data-origin', 'new')
	// The seeded failing async invocation ran twice: one invocation, 2 attempts (1 retry).
	await expect(dlqRow.getByTestId('invocation-attempts')).toContainText(
		'(1 retry)',
	)
	await page.getByTestId('filter-status').selectOption('succeeded')
	await expect(dlqRow).toHaveCount(0)
	await expect(
		page.locator('[data-testid="invocation-row"][data-status="failed"]'),
	).toHaveCount(0)
	await page.getByTestId('filter-status').selectOption('')
	await page.getByTestId('filter-mode').selectOption('async')
	await expect(dlqRow).toBeVisible()
	await shot(page, '07-history')

	await dlqRow.getByRole('link').first().click()
	await expect(
		page.locator('[data-testid="attempt"][data-attempt-kind="retry"]'),
	).toHaveCount(1)
	await expect(page.getByTestId('dead-letter-link')).toBeVisible()
	await shot(page, '08-retried-invocation')
})

test('redrive: confirmation with a reason, result toast, and a new invocation linked to its source', async ({
	page,
}) => {
	await signIn(page, s.tokens.a)
	await page.goto(
		`/console/functions/detail/?id=${s.ids.hello}&tab=dead-letters`,
	)
	const row = page.locator(
		`[data-testid="dead-letter-row"][data-dead-letter-id="${s.ids.deadLetter}"]`,
	)
	await expect(row).toHaveAttribute('data-status', 'open')
	await row.getByRole('link').first().click()
	await expect(page.getByTestId('dead-letter-detail')).toBeVisible()

	// Dismissing the dialog changes nothing.
	await page.getByTestId('redrive-trigger').click()
	await expect(page.getByTestId('redrive-dialog')).toContainText(
		'new asynchronous invocation',
	)
	await page.getByRole('button', { name: 'Cancel' }).click()
	await expect(page.getByTestId('dead-letter-detail')).toHaveAttribute(
		'data-status',
		'open',
	)

	await page.getByTestId('redrive-trigger').click()
	await page.getByTestId('redrive-reason').fill('playwright: downstream fixed')
	await shot(page, '09-redrive-confirm')
	await page.getByTestId('redrive-confirm').click()
	await expect(page.getByTestId('toast-success')).toContainText(
		'Redrive accepted',
	)
	await expect(page.getByTestId('dead-letter-detail')).toHaveAttribute(
		'data-status',
		'redriven',
	)
	await expect(page.getByTestId('redrive-row-reason')).toHaveText(
		'playwright: downstream fixed',
	)
	await expect(page.getByTestId('redrive-trigger')).toBeDisabled()
	await shot(page, '10-redrive-done')

	await page.getByTestId('redrive-result').getByRole('link').click()
	await expect(page.getByTestId('origin-redrive')).toBeVisible()
	await expect(page.getByTestId('redrive-origin')).toContainText(
		s.ids.dlqInvocation,
	)
	await expect(page.getByTestId('redrive-origin')).toContainText(
		'playwright: downstream fixed',
	)
	await shot(page, '11-redriven-invocation')

	await page.goto(
		`/console/functions/detail/?id=${s.ids.hello}&tab=invocations`,
	)
	await page.getByTestId('filter-origin').selectOption('redrive')
	await expect(
		page.locator('[data-testid="invocation-row"][data-origin="redrive"]'),
	).toHaveCount(1)
})

test('rollback: confirmation dialog, result toast, alias back on the previous revision', async ({
	page,
}) => {
	await signIn(page, s.tokens.a)
	await page.goto(`/console/functions/detail/?id=${s.ids.hello}`)
	const prod = page.locator('[data-testid="alias-row"][data-alias="prod"]')
	await expect(prod.getByTestId('alias-revision')).toHaveText('#2')

	await prod.getByTestId('rollback-trigger').click()
	await expect(page.getByTestId('rollback-dialog')).toContainText(s.ids.revV1)
	await page.getByRole('button', { name: 'Cancel' }).click()
	await expect(prod.getByTestId('alias-revision')).toHaveText('#2')

	await prod.getByTestId('rollback-trigger').click()
	await shot(page, '12-rollback-confirm')
	await page.getByTestId('rollback-confirm').click()
	await expect(page.getByTestId('toast-success')).toContainText(
		'Alias prod rolled back',
	)
	await expect(prod.getByTestId('alias-revision')).toHaveText('#1')
	await shot(page, '13-rollback-done')
})

test('cancel: confirmation dialog, result toast, invocation ends cancelled', async ({
	page,
	request,
}) => {
	await signIn(page, s.tokens.a)
	const res = await request.post(
		`/v1/functions/${s.ids.cpuBurn}/invokeAsync?alias=prod`,
		{
			headers: { authorization: `Bearer ${s.tokens.a}` },
			data: { seconds: 40 },
		},
	)
	expect(res.status()).toBe(202)
	const { invocation_id } = (await res.json()) as { invocation_id: string }
	await page.goto(`/console/invocations/detail/?id=${invocation_id}`)
	await expect(page.getByTestId('invocation-status')).toHaveAttribute(
		'data-status',
		'running',
		{ timeout: 60_000 },
	)
	await page.getByTestId('cancel-trigger').click()
	await expect(page.getByTestId('cancel-dialog')).toContainText(invocation_id)
	await shot(page, '14-cancel-confirm')
	await page.getByTestId('cancel-confirm').click()
	await expect(page.getByTestId('toast-success')).toContainText(
		'Invocation cancelled',
	)
	await expect(page.getByTestId('invocation-status')).toHaveAttribute(
		'data-status',
		'cancelled',
	)
	await expect(page.getByTestId('cancel-trigger')).toHaveCount(0)
	await shot(page, '15-cancel-done')
})

test('OutcomeUnknown is explained on the invocation page and on a test invoke (mocked API responses)', async ({
	page,
}) => {
	await signIn(page, s.tokens.a)
	// The process provider cannot be made to lose an in-flight bridge on demand,
	// so the API's documented outcome_unknown shapes are served by a route mock.
	const id = 'inv_01mockoutcomeunknown000000'
	await page.route(`**/v1/invocations/${id}`, route =>
		route.fulfill({
			json: {
				id,
				function_id: s.ids.hello,
				revision_id: s.ids.revV2,
				alias: 'prod',
				mode: 'sync',
				status: 'outcome_unknown',
				trace_id: id,
				input_digest: 'sha256:0000',
				input_size_bytes: 2,
				accepted_at: '2026-09-17T00:00:00Z',
				started_at: '2026-09-17T00:00:00.1Z',
				finished_at: '2026-09-17T00:00:03Z',
				deadlines: {
					queue_deadline: '2026-09-17T00:00:10Z',
					client_deadline: '2026-09-17T00:01:00Z',
				},
				error: {
					class: 'outcome_unknown',
					error_type: 'Host.BridgeDisconnected',
					message: 'connection lost after Invoke',
				},
				attempts: [],
			},
		}),
	)
	await page.route(`**/v1/invocations/${id}/logs`, route =>
		route.fulfill({ json: { items: [], dropped: false } }),
	)
	await page.goto(`/console/invocations/detail/?id=${id}`)
	await expect(page.getByTestId('invocation-status')).toHaveAttribute(
		'data-status',
		'outcome_unknown',
	)
	await expect(page.getByTestId('outcome-unknown-notice')).toContainText(
		'never re-runs a synchronous invocation',
	)
	await shot(page, '16-outcome-unknown')

	await page.route('**/v1/functions/*/invoke?*', route =>
		route.fulfill({
			status: 502,
			headers: { 'x-tachyon-invocation-id': id },
			json: {
				error: {
					code: 'outcome_unknown',
					message: 'the outcome of the invocation is unknown',
					invocation_id: id,
					error_type: 'Host.BridgeDisconnected',
					request_id: 'req_mock',
				},
			},
		}),
	)
	await page.goto(`/console/functions/detail/?id=${s.ids.hello}&tab=invoke`)
	await page.getByTestId('invoke-submit').click()
	await expect(
		page
			.getByTestId('invoke-result-error')
			.getByTestId('outcome-unknown-notice'),
	).toBeVisible()
	await expect(page.getByTestId('invoke-error-code')).toHaveText(
		'outcome_unknown',
	)
})

test('permission denied: an operator token reads functions but not invocations or usage', async ({
	page,
}) => {
	await signIn(page, s.tokens.operator)
	await expect(page.getByTestId('function-row')).toHaveCount(2)
	await page.goto(
		`/console/functions/detail/?id=${s.ids.hello}&tab=invocations`,
	)
	await expect(page.getByTestId('state-forbidden')).toContainText('HTTP 403')
	await shot(page, '17-permission-denied')
	await page.goto('/console/usage/')
	await expect(page.getByTestId('state-forbidden')).toBeVisible()
	await page.goto(`/console/functions/detail/?id=${s.ids.hello}&tab=invoke`)
	await page.getByTestId('invoke-submit').click()
	await expect(page.getByTestId('invoke-error-code')).toHaveText('forbidden')
})

test('cross-tenant URLs answer not found for functions, invocations and dead letters', async ({
	page,
}) => {
	await signIn(page, s.tokens.a)
	await page.goto(`/console/functions/detail/?id=${s.ids.bFunction}`)
	await expect(page.getByTestId('state-not-found')).toContainText('HTTP 404')
	await shot(page, '18-cross-tenant-function')
	await page.goto(`/console/invocations/detail/?id=${s.ids.bInvocation}`)
	await expect(page.getByTestId('state-not-found')).toBeVisible()
	await expect(page.locator('body')).not.toContainText('tenant-b-private')

	await page.getByTestId('sign-out').click()
	await signIn(page, s.tokens.b)
	await page.goto(`/console/dead-letters/detail/?id=${s.ids.deadLetter}`)
	await expect(page.getByTestId('state-not-found')).toBeVisible()
	await page.goto(`/console/invocations/detail/?id=${s.ids.seedInvocation}`)
	await expect(page.getByTestId('state-not-found')).toBeVisible()
	await page.goto(
		`/console/functions/detail/?id=${s.ids.hello}&tab=invocations`,
	)
	await expect(page.getByTestId('state-not-found').first()).toBeVisible()
})

test('usage and budget show the provisional banner; budget state; capacity renders', async ({
	page,
}) => {
	await signIn(page, s.tokens.a)
	await page.goto('/console/usage/')
	await expect(page.getByTestId('provisional-banner')).toContainText(
		'not an invoice',
	)
	await expect(page.getByTestId('usage-report')).toBeVisible()
	await expect(page.getByTestId('usage-total')).toContainText('JPY')
	await expect(page.getByTestId('provisional-banner')).toContainText(
		'API notice',
	)
	await shot(page, '19-usage')
	await expectNotInDom(page, secrets())

	await page.goto('/console/budget/')
	await expect(page.getByTestId('provisional-banner')).toContainText(
		'not an invoice',
	)
	await expect(page.getByTestId('budget-admitting')).toHaveAttribute(
		'data-admitting',
		'true',
	)
	await expect(page.getByTestId('budget-hard-limit')).toContainText(
		'1000000.000000 JPY',
	)
	// The tiny soft limit of the seed is exceeded by the invocations above.
	await expect(page.getByTestId('budget-alerts')).toContainText('100%', {
		timeout: 30_000,
	})
	await expect(page.getByTestId('budget-guarantee')).not.toBeEmpty()
	await shot(page, '20-budget')

	// A gateway without the budget API (before PLT-4643) answers the fallback 404 (mocked).
	await page.route('**/v1/budget', route =>
		route.fulfill({
			status: 404,
			json: {
				error: {
					code: 'not_found',
					message: 'not found: no route for GET /v1/budget',
				},
			},
		}),
	)
	await page.goto('/console/budget/')
	await expect(page.getByTestId('provisional-banner')).toBeVisible()
	await expect(page.getByTestId('state-unavailable')).toBeVisible()
	await page.unroute('**/v1/budget')

	await page.goto('/console/capacity/')
	await expect(page.getByTestId('capacity')).toContainText(s.tenants.a)
	await shot(page, '21-capacity')
})

test('loading, empty, error and unauthorized states (mocked API responses)', async ({
	page,
}) => {
	await signIn(page, s.tokens.a)
	let release: () => void = () => {}
	const gate = new Promise<void>(r => {
		release = r
	})
	await page.route('**/v1/functions', async route => {
		await gate
		await route.fulfill({ json: { items: [] } })
	})
	await page.goto('/console/functions/')
	await expect(page.getByTestId('state-loading')).toBeVisible()
	release()
	await expect(page.getByTestId('state-empty')).toContainText(
		'No functions yet',
	)
	await page.unroute('**/v1/functions')

	await page.route('**/v1/functions', route =>
		route.fulfill({
			status: 500,
			json: {
				error: {
					code: 'platform_error',
					message: 'store unavailable',
					request_id: 'req_mock',
				},
			},
		}),
	)
	await page.goto('/console/functions/')
	await expect(page.getByTestId('state-error')).toContainText(
		'store unavailable',
	)
	await shot(page, '22-error-state')
	await page.unroute('**/v1/functions')

	await page.route('**/v1/functions', route =>
		route.fulfill({
			status: 401,
			json: { error: { code: 'unauthorized', message: 'unknown credential' } },
		}),
	)
	await page.goto('/console/functions/')
	await expect(page.getByTestId('state-unauthorized')).toBeVisible()
	await page.unroute('**/v1/functions')
})

test('no secret value or token anywhere in the DOM across the main pages', async ({
	page,
}) => {
	await signIn(page, s.tokens.a)
	const pages = [
		'/console/functions/',
		`/console/functions/detail/?id=${s.ids.hello}`,
		`/console/functions/detail/?id=${s.ids.hello}&tab=invocations`,
		`/console/functions/detail/?id=${s.ids.hello}&tab=dead-letters`,
		`/console/functions/detail/?id=${s.ids.hello}&tab=usage`,
		`/console/invocations/detail/?id=${s.ids.seedInvocation}`,
		`/console/dead-letters/detail/?id=${s.ids.deadLetter}`,
		'/console/usage/',
		'/console/capacity/',
	]
	for (const p of pages) {
		await page.goto(p)
		await page.waitForLoadState('networkidle')
		await expect(page.getByTestId('state-loading')).toHaveCount(0)
		// Expand everything that can be expanded.
		// Each click relabels its button ("Show …" → "Hide …"), so always take the first one left.
		const expanders = page.getByRole('button', {
			name: /Show values|Show output|Show boot evidence/,
		})
		for (let i = 0; i < 20 && (await expanders.count()) > 0; i++) {
			await expanders.first().click()
		}
		await expectNotInDom(page, secrets())
		expect(page.url()).not.toContain(s.tokens.a)
	}
	// The token lives in sessionStorage of this tab only, never in localStorage or cookies.
	const storage = await page.evaluate(() => ({
		local: JSON.stringify(window.localStorage),
		cookies: document.cookie,
	}))
	expect(storage.local).not.toContain(s.tokens.a)
	expect(storage.cookies).not.toContain(s.tokens.a)
})
