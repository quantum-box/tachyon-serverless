// Console routes. The static export cannot pre-render one page per id, so
// ids travel as query parameters (`/functions/detail/?id=fn_…`). In the
// Tachyon Console these become path segments
// (`/v1beta/[tenant_id]/serverless/functions/[function_id]`), see
// docs/console-integration.md.

export const routes = {
	signIn: '/',
	functions: '/functions/',
	function: (id: string, tab?: FunctionTab) =>
		`/functions/detail/?id=${encodeURIComponent(id)}${tab && tab !== 'overview' ? `&tab=${tab}` : ''}`,
	revision: (functionId: string, revisionId: string) =>
		`/functions/detail/?id=${encodeURIComponent(functionId)}#revision-${encodeURIComponent(revisionId)}`,
	invocation: (id: string, attemptId?: string) =>
		`/invocations/detail/?id=${encodeURIComponent(id)}${attemptId ? `#attempt-${encodeURIComponent(attemptId)}` : ''}`,
	invocationLogs: (id: string, attemptId?: string) =>
		`/invocations/detail/?id=${encodeURIComponent(id)}${attemptId ? `&attempt=${encodeURIComponent(attemptId)}` : ''}#logs`,
	deadLetter: (id: string) =>
		`/dead-letters/detail/?id=${encodeURIComponent(id)}`,
	usage: '/usage/',
	budget: '/budget/',
	capacity: '/capacity/',
} as const

export const FUNCTION_TABS = [
	'overview',
	'invoke',
	'invocations',
	'dead-letters',
	'usage',
] as const
export type FunctionTab = (typeof FUNCTION_TABS)[number]

export function parseFunctionTab(value: string | null): FunctionTab {
	return (FUNCTION_TABS as readonly string[]).includes(value ?? '')
		? (value as FunctionTab)
		: 'overview'
}

/** Only same-app relative paths are accepted as a post-sign-in destination. */
export function safeNextPath(value: string | null | undefined): string {
	if (!value) return routes.functions
	if (
		!value.startsWith('/') ||
		value.startsWith('//') ||
		value.includes('\\')
	) {
		return routes.functions
	}
	if (value === '/' || value.startsWith('/?')) return routes.functions
	return value
}
