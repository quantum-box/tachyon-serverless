// Fails when src/gen/openapi/serverless-api.ts is not what `pnpm gen:api`
// produces from ../../docs/openapi.json (the gateway's OpenAPI snapshot).
import { execFileSync } from 'node:child_process'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')
const dir = mkdtempSync(join(tmpdir(), 'tsls-console-api-'))
try {
	const out = join(dir, 'serverless-api.ts')
	execFileSync(
		join(root, 'node_modules', '.bin', 'openapi-typescript'),
		[join(root, '..', '..', 'docs', 'openapi.json'), '-o', out],
		{ stdio: ['ignore', 'ignore', 'inherit'] },
	)
	const expected = readFileSync(out, 'utf8')
	const actual = readFileSync(join(root, 'src', 'gen', 'openapi', 'serverless-api.ts'), 'utf8')
	if (expected !== actual) {
		console.error('src/gen/openapi/serverless-api.ts is stale: run `pnpm gen:api` and commit the result')
		process.exit(1)
	}
	console.log('API types match docs/openapi.json')
} finally {
	rmSync(dir, { recursive: true, force: true })
}
