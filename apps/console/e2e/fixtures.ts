import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { type Page, expect } from '@playwright/test'

export interface Seed {
	baseUrl: string
	tokens: { a: string; operator: string; b: string }
	tenants: { a: string; b: string }
	secretValue: string
	ids: {
		hello: string
		cpuBurn: string
		revV1: string
		revV2: string
		revFailed: string
		seedInvocation: string
		dlqInvocation: string
		deadLetter: string
		bFunction: string
		bInvocation: string
	}
}

export function seed(): Seed {
	const path = process.env.CONSOLE_E2E_SEED
	if (!path)
		throw new Error('CONSOLE_E2E_SEED is not set: run scripts/console/e2e.sh')
	return JSON.parse(readFileSync(path, 'utf8')) as Seed
}

export async function signIn(page: Page, token: string, tenantId?: string) {
	await page.goto('/console/')
	await page.getByLabel('API token').fill(token)
	if (tenantId) await page.getByLabel('Tenant id (optional)').fill(tenantId)
	await page.getByRole('button', { name: 'Sign in' }).click()
	await expect(page).toHaveURL(/\/console\/functions\/$/)
}

export async function shot(page: Page, name: string) {
	const dir = process.env.CONSOLE_E2E_EVIDENCE
	if (!dir) return
	await page.screenshot({
		path: join(dir, 'screenshots', `${name}.png`),
		fullPage: true,
	})
}

/** The rendered DOM and the visible text must not contain any of `needles`. */
export async function expectNotInDom(page: Page, needles: string[]) {
	const html = await page.content()
	const text = await page.locator('body').innerText()
	for (const n of needles) {
		expect(
			html.includes(n),
			`DOM contains a forbidden value (${n.slice(0, 12)}…)`,
		).toBe(false)
		expect(text.includes(n)).toBe(false)
	}
}
