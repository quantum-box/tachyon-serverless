'use client'

import useSWR, { type SWRConfiguration, type SWRResponse } from 'swr'
import { useCredentials } from '../session'
import {
	type ApiError,
	type RequestOptions,
	apiRequest,
	toApiError,
} from './client'

/**
 * GET `path` with the session's credentials. `null` skips the request.
 * The SWR key includes a fingerprint of the token (not the token) so that
 * signing in with another token never shows the previous tenant's cache.
 */
export function useApi<T>(
	path: string | null,
	query?: RequestOptions['query'],
	config?: SWRConfiguration<T, ApiError>,
): SWRResponse<T, ApiError> {
	const credentials = useCredentials()
	const key = path
		? ([
				path,
				JSON.stringify(query ?? {}),
				fingerprint(credentials.token),
				credentials.tenantId ?? '',
			] as const)
		: null
	return useSWR<T, ApiError>(
		key,
		async () => {
			try {
				const res = await apiRequest<T>(credentials, path as string, { query })
				return res.data
			} catch (e) {
				throw toApiError(e)
			}
		},
		{
			revalidateOnFocus: false,
			shouldRetryOnError: false,
			...config,
		},
	)
}

/** Non-reversible, short cache-key fingerprint of a token (FNV-1a). */
export function fingerprint(token: string): string {
	let h = 0x811c9dc5
	for (let i = 0; i < token.length; i++) {
		h ^= token.charCodeAt(i)
		h = Math.imul(h, 0x01000193)
	}
	return (h >>> 0).toString(16)
}
