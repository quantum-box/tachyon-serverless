import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { defineConfig, devices } from '@playwright/test'

// Run through scripts/console/e2e.sh, which starts a real gateway serving the
// built console and writes the seed file (ids and throwaway tokens).
const seedPath = process.env.CONSOLE_E2E_SEED
const evidence = process.env.CONSOLE_E2E_EVIDENCE ?? 'test-results/evidence'
const baseURL = seedPath
	? (JSON.parse(readFileSync(seedPath, 'utf8')) as { baseUrl: string }).baseUrl
	: 'http://127.0.0.1:8080'

export default defineConfig({
	testDir: './e2e',
	// One gateway with shared state: the specs run in order, one at a time.
	fullyParallel: false,
	workers: 1,
	retries: 0,
	timeout: 90_000,
	expect: { timeout: 20_000 },
	outputDir: join(evidence, 'test-output'),
	reporter: [
		['list'],
		['json', { outputFile: join(evidence, 'results.json') }],
		['html', { outputFolder: join(evidence, 'playwright-report'), open: 'never' }],
	],
	use: {
		baseURL,
		headless: true,
		screenshot: 'only-on-failure',
		// Traces record request headers (the bearer token): never written, so
		// the evidence directory cannot carry a credential.
		trace: 'off',
		video: 'off',
	},
	projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
})
