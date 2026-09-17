import { afterEach, describe, expect, it, vi } from 'vitest'
import { filterInvocations } from '../components/functions/invocations-panel'
import { formatMicros, shortId } from './format'
import { attemptKind, invocationOrigin, retryCount } from './invocation-kind'
import { parseFunctionTab, routes, safeNextPath } from './navigation'
import {
	ApiError,
	apiPath,
	apiRequest,
	buildUrl,
} from './serverless-api/client'
import { fingerprint } from './serverless-api/hooks'
import type { InvocationResponse } from './serverless-api/types'

function inv(over: Partial<InvocationResponse>): InvocationResponse {
	return {
		id: 'inv_1',
		function_id: 'fn_1',
		revision_id: 'rev_1',
		mode: 'sync',
		status: 'succeeded',
		trace_id: 't',
		input_digest: 'sha256:x',
		input_size_bytes: 2,
		accepted_at: '2026-09-17T00:00:00Z',
		deadlines: {
			queue_deadline: '2026-09-17T00:00:10Z',
			client_deadline: '2026-09-17T00:01:00Z',
		},
		...over,
	} as InvocationResponse
}

describe('retry vs new invocation', () => {
	it('an invocation without redriven_from is a new invocation', () => {
		expect(invocationOrigin(inv({})).kind).toBe('new')
	})

	it('a redriven invocation points at its source and dead letter', () => {
		const o = invocationOrigin(
			inv({
				mode: 'async',
				dispatch: {
					state: 'done',
					attempts: 1,
					deferrals: 0,
					generation: 0,
					redriven_from: {
						id: 'rdr_1',
						dead_letter_id: 'dlq_1',
						function_id: 'fn_1',
						source_invocation_id: 'inv_0',
						invocation_id: 'inv_1',
						revision_id: 'rev_1',
						revision_overridden: false,
						requested_by: 'oncall',
						reason: 'fixed',
						created_at: '2026-09-17T00:00:00Z',
					},
				} as InvocationResponse['dispatch'],
			}),
		)
		expect(o).toMatchObject({
			kind: 'redrive',
			sourceInvocationId: 'inv_0',
			deadLetterId: 'dlq_1',
		})
	})

	it('attempt 1 is the initial run and later attempts are retries', () => {
		expect(attemptKind({ number: 1 })).toBe('initial')
		expect(attemptKind({ number: 2 })).toBe('retry')
	})

	it('counts retries from recorded attempts or the dispatch counter', () => {
		expect(retryCount(inv({ attempts: [] }))).toBe(0)
		expect(
			retryCount(
				inv({ dispatch: { attempts: 3 } as InvocationResponse['dispatch'] }),
			),
		).toBe(2)
	})

	it('filters history by status, mode and origin', () => {
		const rows = [
			inv({ id: 'a', status: 'failed' }),
			inv({ id: 'b', status: 'outcome_unknown', mode: 'async' }),
		]
		expect(
			filterInvocations(rows, {
				status: 'outcome_unknown',
				mode: '',
				origin: '',
				revision: '',
			}).map(r => r.id),
		).toEqual(['b'])
		expect(
			filterInvocations(rows, {
				status: '',
				mode: 'sync',
				origin: 'new',
				revision: '',
			}).map(r => r.id),
		).toEqual(['a'])
		expect(
			filterInvocations(rows, {
				status: '',
				mode: '',
				origin: 'redrive',
				revision: '',
			}),
		).toEqual([])
	})
})

describe('navigation', () => {
	it('only accepts same-app paths after sign-in', () => {
		expect(safeNextPath('/invocations/detail/?id=inv_1')).toBe(
			'/invocations/detail/?id=inv_1',
		)
		expect(safeNextPath('//evil.example/x')).toBe(routes.functions)
		expect(safeNextPath('https://evil.example')).toBe(routes.functions)
		expect(safeNextPath('/\\evil')).toBe(routes.functions)
		expect(safeNextPath(null)).toBe(routes.functions)
	})

	it('encodes ids in routes', () => {
		expect(routes.invocation('inv_1&x=1')).toBe(
			'/invocations/detail/?id=inv_1%26x%3D1',
		)
		expect(parseFunctionTab('nope')).toBe('overview')
		expect(parseFunctionTab('dead-letters')).toBe('dead-letters')
	})
})

describe('format', () => {
	it('prints micros without rounding', () => {
		expect(formatMicros(8897, 'JPY')).toBe('0.008897 JPY')
		expect(formatMicros(12_000_001, 'JPY')).toBe('12.000001 JPY')
	})
	it('shortens ids', () => {
		expect(shortId('inv_01j7z2k3m4n5p6q7r8s9t0v1w2')).toBe('inv_01j7…v1w2')
	})
})

describe('api client', () => {
	afterEach(() => vi.unstubAllGlobals())

	it('refuses paths outside /v1 and encodes path segments', () => {
		expect(() => buildUrl('https://evil.example/v1/functions')).toThrow()
		expect(() => buildUrl('/metrics')).toThrow()
		expect(apiPath`/v1/functions/${'a/../b'}`).toBe('/v1/functions/a%2F..%2Fb')
		expect(
			buildUrl('/v1/usage', { from: '', group_by: 'day', to: undefined }),
		).toBe('/v1/usage?group_by=day')
	})

	it('sends the token only as a bearer header and maps API errors', async () => {
		const fetchMock = vi.fn(
			async (_url: string, _init: RequestInit) =>
				new Response(
					JSON.stringify({
						error: {
							code: 'forbidden',
							message: 'missing role invoke',
							request_id: 'req_1',
						},
					}),
					{ status: 403, headers: { 'content-type': 'application/json' } },
				),
		)
		vi.stubGlobal('fetch', fetchMock)
		const err = await apiRequest(
			{ token: 'secret-token-value', tenantId: 'tn_a' },
			'/v1/usage',
		).catch(e => e)
		expect(err).toBeInstanceOf(ApiError)
		expect(err.status).toBe(403)
		expect(err.code).toBe('forbidden')
		expect(err.requestId).toBe('req_1')
		expect(String(err.message)).not.toContain('secret-token-value')
		const [url, init] = fetchMock.mock.calls[0]!
		expect(url).toBe('/v1/usage')
		expect(url).not.toContain('secret-token-value')
		const headers = init.headers as Record<string, string>
		expect(headers.authorization).toBe('Bearer secret-token-value')
		expect(headers['x-tachyon-tenant-id']).toBe('tn_a')
		expect(init.credentials).toBe('omit')
	})

	it('recognises a gateway without a route', () => {
		const e = new ApiError({
			status: 404,
			code: 'not_found',
			message: 'no route for GET /v1/budget',
		})
		expect(e.isMissingRoute).toBe(true)
		expect(
			new ApiError({
				status: 404,
				code: 'not_found',
				message: 'function not found',
			}).isMissingRoute,
		).toBe(false)
	})

	it('cache keys never contain the token', () => {
		const f = fingerprint('dev-token-tenant-a')
		expect(f).not.toContain('dev-token')
		expect(f).not.toBe(fingerprint('dev-token-tenant-b'))
	})
})

describe('history origin without a dispatch section', () => {
	it('uses redrive records from dead letters when the list lacks dispatch', () => {
		const known = new Map([
			[
				'inv_2',
				{
					id: 'rdr_1',
					dead_letter_id: 'dlq_1',
					function_id: 'fn_1',
					source_invocation_id: 'inv_1',
					invocation_id: 'inv_2',
					revision_id: 'rev_1',
					revision_overridden: false,
					requested_by: 'oncall',
					reason: null,
					created_at: '2026-09-17T00:00:00Z',
				},
			],
		])
		expect(
			invocationOrigin(inv({ id: 'inv_2', mode: 'async' }), known).kind,
		).toBe('redrive')
		expect(
			invocationOrigin(inv({ id: 'inv_3', mode: 'async' }), known).kind,
		).toBe('new')
		expect(
			filterInvocations(
				[inv({ id: 'inv_2' }), inv({ id: 'inv_3' })],
				{ status: '', mode: '', origin: 'redrive', revision: '' },
				known,
			).map(r => r.id),
		).toEqual(['inv_2'])
	})
})
