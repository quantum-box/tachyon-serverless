// Minimal fetch client of the Tachyon Serverless management API.
//
// - Same origin only: the console is served by the gateway (or proxied by
//   `next dev`), so every path is a relative `/v1/...` URL. There is no base
//   URL setting that could point the token at another host.
// - The token is sent as `Authorization: Bearer` and, when the viewer typed
//   one, the tenant id as `x-tachyon-tenant-id` (the API refuses a mismatch).
// - The token is never logged, put in a URL, or included in an error.
// - Tenant isolation is the API's job: another tenant's resource answers 404
//   whatever the console asks for.

import type { ApiErrorBody } from './types'

export interface Credentials {
	token: string
	tenantId?: string
}

export class ApiError extends Error {
	readonly status: number
	readonly code: string
	readonly reason?: string | null
	readonly errorType?: string | null
	readonly invocationId?: string | null
	readonly requestId?: string | null

	constructor(init: {
		status: number
		code: string
		message: string
		reason?: string | null
		errorType?: string | null
		invocationId?: string | null
		requestId?: string | null
	}) {
		super(init.message)
		this.name = 'ApiError'
		this.status = init.status
		this.code = init.code
		this.reason = init.reason
		this.errorType = init.errorType
		this.invocationId = init.invocationId
		this.requestId = init.requestId
	}

	/** 404 from a gateway that does not have the route at all (as opposed to a missing resource). */
	get isMissingRoute(): boolean {
		// The gateway's fallback answers `not found: no route for GET /v1/...`.
		return this.status === 404 && this.message.includes('no route for ')
	}
}

export interface ApiResponse<T> {
	status: number
	data: T
	headers: Headers
}

export interface RequestOptions {
	method?: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE'
	query?: Record<string, string | number | undefined | null>
	body?: unknown
	headers?: Record<string, string>
	signal?: AbortSignal
}

/** `/v1/...` path with every dynamic segment percent-encoded. */
export function apiPath(
	strings: TemplateStringsArray,
	...values: (string | number)[]
): string {
	let out = ''
	strings.forEach((s, i) => {
		out += s
		if (i < values.length) out += encodeURIComponent(String(values[i]))
	})
	return out
}

export function buildUrl(
	path: string,
	query?: RequestOptions['query'],
): string {
	if (!path.startsWith('/v1/')) {
		throw new Error('the console only calls the /v1 management API')
	}
	const params = new URLSearchParams()
	for (const [k, v] of Object.entries(query ?? {})) {
		if (v !== undefined && v !== null && v !== '') params.set(k, String(v))
	}
	const qs = params.toString()
	return qs ? `${path}?${qs}` : path
}

export async function apiRequest<T>(
	credentials: Credentials,
	path: string,
	options: RequestOptions = {},
): Promise<ApiResponse<T>> {
	const headers: Record<string, string> = {
		accept: 'application/json',
		authorization: `Bearer ${credentials.token}`,
		...(options.headers ?? {}),
	}
	if (credentials.tenantId)
		headers['x-tachyon-tenant-id'] = credentials.tenantId
	let body: string | undefined
	if (options.body !== undefined) {
		headers['content-type'] = 'application/json'
		body = JSON.stringify(options.body)
	}
	let res: Response
	try {
		res = await fetch(buildUrl(path, options.query), {
			method: options.method ?? 'GET',
			headers,
			body,
			signal: options.signal,
			credentials: 'omit',
			cache: 'no-store',
			redirect: 'error',
		})
	} catch (e) {
		throw new ApiError({
			status: 0,
			code: 'network_error',
			message:
				e instanceof Error && e.name === 'AbortError'
					? 'request aborted'
					: 'the gateway could not be reached',
		})
	}
	const text = await res.text()
	let parsed: unknown = undefined
	if (text) {
		try {
			parsed = JSON.parse(text)
		} catch {
			parsed = undefined
		}
	}
	if (!res.ok) {
		const err = (parsed as ApiErrorBody | undefined)?.error
		throw new ApiError({
			status: res.status,
			code: err?.code ?? `http_${res.status}`,
			message: err?.message ?? `HTTP ${res.status}`,
			reason: err?.reason,
			errorType: err?.error_type,
			invocationId:
				err?.invocation_id ?? res.headers.get('x-tachyon-invocation-id'),
			requestId: err?.request_id ?? res.headers.get('x-request-id'),
		})
	}
	return { status: res.status, data: parsed as T, headers: res.headers }
}

export function toApiError(e: unknown): ApiError {
	if (e instanceof ApiError) return e
	return new ApiError({
		status: 0,
		code: 'client_error',
		message: e instanceof Error ? e.message : 'unexpected error',
	})
}
