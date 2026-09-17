// Types of the public management API, generated from docs/openapi.json
// (`pnpm gen:api`; `pnpm check:api` fails when the generated file is stale).
// The gateway's OpenAPI document is the source of truth; nothing here adds a
// field the API does not have.
import type { components } from '@/gen/openapi/serverless-api'

type Schemas = components['schemas']

export type ApiErrorBody = Schemas['ApiErrorBody']
export type ErrorCode = Schemas['ErrorCode']
export type FunctionResponse = Schemas['FunctionResponse']
export type RevisionResponse = Schemas['RevisionResponse']
export type AliasResponse = Schemas['AliasResponse']
export type InvocationResponse = Schemas['InvocationResponse']
export type AttemptResponse = Schemas['AttemptResponse']
export type TimingsResponse = Schemas['TimingsResponse']
export type InvocationErrorResponse = Schemas['InvocationErrorResponse']
export type AsyncDispatchResponse = Schemas['AsyncDispatchResponse']
export type LogsResponse = Schemas['LogsResponse']
export type LogEntryResponse = Schemas['LogEntryResponse']
export type DeadLetterResponse = Schemas['DeadLetterResponse']
export type RedriveResponse = Schemas['RedriveResponse']
export type RedriveAcceptedResponse = Schemas['RedriveAcceptedResponse']
export type InvokeAsyncResponse = Schemas['InvokeAsyncResponse']
export type UsageReportResponse = Schemas['UsageReportResponse']
export type UsageReportLine = Schemas['UsageReportLine']
export type UsageSummaryResponse = Schemas['UsageSummaryResponse']
export type CapacityInfo = Schemas['CapacityInfo']
export type RevisionCapacityInfo = Schemas['RevisionCapacityInfo']
export type BudgetReportResponse = Schemas['BudgetReportResponse']
export type BudgetScopeReport = Schemas['BudgetScopeReport']

export interface ListResponse<T> {
	items: T[]
	next_cursor?: string | null
}

/** Terminal invocation statuses (docs/api.md §5.8). */
export const TERMINAL_STATUSES = [
	'succeeded',
	'failed',
	'cancelled',
	'outcome_unknown',
] as const

export function isTerminal(status: string): boolean {
	return (TERMINAL_STATUSES as readonly string[]).includes(status)
}

/** Revision statuses that are still moving (`pending` → … → `ready` | `failed`). */
export function isRevisionSettling(status: string): boolean {
	return status !== 'ready' && status !== 'failed'
}
