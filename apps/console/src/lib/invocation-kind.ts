// How the console tells a *retry* from a *new invocation* (PLT-4644).
//
// The API has two different things that are easy to confuse:
// - a retry is another **attempt of the same invocation** (same invocation id,
//   attempt number > 1). Asynchronous invocations retry on their own under
//   `[async_dispatch]`; a synchronous invocation is never re-executed after
//   dispatch, the only extra attempt is the cold re-run after a warm
//   environment failed before the handler started (docs/api.md §6).
// - a redrive, or pressing "Invoke" again, creates a **new invocation** with
//   its own id. A redriven invocation carries `dispatch.redriven_from` with
//   the source invocation and the dead letter.
import type {
	AttemptResponse,
	InvocationResponse,
	RedriveResponse,
} from './serverless-api/types'

export type InvocationOrigin =
	| { kind: 'new' }
	| {
			kind: 'redrive'
			sourceInvocationId: string
			deadLetterId: string
			redriveId: string
			requestedBy: string
			reason: string | null | undefined
			revisionOverridden: boolean
	  }

/**
 * Origin of an invocation. `GET /v1/invocations/{id}` carries
 * `dispatch.redriven_from`; the history list does not carry `dispatch` at all,
 * so the list passes the redrive records of the function's dead letters
 * (`knownRedrives`, keyed by the new invocation id) instead.
 */
export function invocationOrigin(
	inv: Pick<InvocationResponse, 'dispatch' | 'id'>,
	knownRedrives?: ReadonlyMap<string, RedriveResponse>,
): InvocationOrigin {
	const r = inv.dispatch?.redriven_from ?? knownRedrives?.get(inv.id)
	if (!r) return { kind: 'new' }
	return {
		kind: 'redrive',
		sourceInvocationId: r.source_invocation_id,
		deadLetterId: r.dead_letter_id,
		redriveId: r.id,
		requestedBy: r.requested_by,
		reason: r.reason,
		revisionOverridden: r.revision_overridden,
	}
}

export type AttemptKind = 'initial' | 'retry'

export function attemptKind(
	attempt: Pick<AttemptResponse, 'number'>,
): AttemptKind {
	return attempt.number <= 1 ? 'initial' : 'retry'
}

/** Number of retries (attempts after the first) an invocation went through. */
export function retryCount(
	inv: Pick<InvocationResponse, 'attempts' | 'dispatch'>,
): number {
	const recorded = inv.attempts?.length ?? 0
	const counted = inv.dispatch?.attempts ?? 0
	return Math.max(0, Math.max(recorded, counted) - 1)
}

export const OUTCOME_UNKNOWN_EXPLANATION =
	'The platform dispatched this invocation to the handler but could not confirm how it ended ' +
	'(for example the connection to the environment was lost after the request was sent, or the ' +
	'gateway that ran it lost its lease). The handler may or may not have completed, and its side ' +
	'effects may or may not have happened.'

export function outcomeUnknownFollowUp(mode: string | undefined): string {
	if (mode === 'async') {
		return (
			'This is an asynchronous invocation: an unknown outcome counts as a failed attempt and is ' +
			'retried under the retry policy (a retry is another attempt of this same invocation). When ' +
			'the attempts run out it ends in a dead letter. Handlers must make side effects idempotent.'
		)
	}
	return (
		'The platform never re-runs a synchronous invocation on its own. Check the handler’s own ' +
		'records before invoking again; invoking again creates a new invocation.'
	)
}
