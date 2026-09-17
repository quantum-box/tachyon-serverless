'use client'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from '@/components/ui/table'
import { formatDateTime, formatMs, shortId } from '@/lib/format'
import { invocationOrigin, retryCount } from '@/lib/invocation-kind'
import { routes } from '@/lib/navigation'
import { apiPath, apiRequest } from '@/lib/serverless-api/client'
import { fingerprint, useApi } from '@/lib/serverless-api/hooks'
import { useCredentials } from '@/lib/session'
import useSWR from 'swr'
import {
	type FunctionResponse,
	type InvocationResponse,
	type ListResponse,
	type RevisionResponse,
	type DeadLetterResponse,
	type RedriveResponse,
	isTerminal,
} from '@/lib/serverless-api/types'
import { RefreshCw } from 'lucide-react'
import Link from 'next/link'
import { useMemo, useState } from 'react'
import { DataState, EmptyState } from './data-state'
import { Mono } from './kv'
import { InvocationStatusBadge } from './status-badge'

const STATUSES = [
	'accepted',
	'queued',
	'running',
	'succeeded',
	'failed',
	'cancelled',
	'outcome_unknown',
]
const selectClass =
	'flex h-9 rounded-md border border-input bg-background px-2 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'

export interface InvocationFilters {
	status: string
	mode: string
	origin: string
	revision: string
}

export function filterInvocations(
	items: InvocationResponse[],
	f: InvocationFilters,
	knownRedrives?: ReadonlyMap<string, RedriveResponse>,
): InvocationResponse[] {
	return items.filter(
		inv =>
			(f.status === '' || inv.status === f.status) &&
			(f.mode === '' || inv.mode === f.mode) &&
			(f.origin === '' ||
				invocationOrigin(inv, knownRedrives).kind === f.origin) &&
			(f.revision === '' || inv.revision_id === f.revision),
	)
}

/** At most this many redriven dead letters are read to label the history. */
const REDRIVE_LOOKUP_CAP = 50

/**
 * Redrive records of the function, keyed by the invocation each redrive
 * created. Neither the history list (no `dispatch`) nor the dead-letter list
 * (empty `redrives`) carries them, so the dead letters with
 * `redrive_count > 0` are read one by one (capped). Without the `invoke`
 * role, or on a gateway without asynchronous invoke, the map stays empty and
 * redriven invocations are labelled from their detail page only.
 */
function useKnownRedrives(
	functionId: string,
): ReadonlyMap<string, RedriveResponse> {
	const credentials = useCredentials()
	const deadLetters = useApi<ListResponse<DeadLetterResponse>>(
		apiPath`/v1/functions/${functionId}/dead-letters`,
		{ limit: 500 },
	)
	const redriven = (deadLetters.data?.items ?? [])
		.filter(d => d.redrive_count > 0)
		.slice(0, REDRIVE_LOOKUP_CAP)
		.map(d => d.id)
	const details = useSWR(
		redriven.length
			? ['redrives', fingerprint(credentials.token), ...redriven]
			: null,
		() =>
			Promise.all(
				redriven.map(id =>
					apiRequest<DeadLetterResponse>(
						credentials,
						apiPath`/v1/dead-letters/${id}`,
					)
						.then(r => r.data)
						.catch(() => null),
				),
			),
		{ revalidateOnFocus: false },
	)
	return useMemo(() => {
		const m = new Map<string, RedriveResponse>()
		for (const d of details.data ?? []) {
			for (const r of d?.redrives ?? []) m.set(r.invocation_id, r)
		}
		return m
	}, [details.data])
}

function durationMs(inv: InvocationResponse): number | null {
	if (!inv.finished_at) return null
	const start = Date.parse(inv.started_at ?? inv.accepted_at)
	const end = Date.parse(inv.finished_at)
	return Number.isNaN(start) || Number.isNaN(end)
		? null
		: Math.max(0, end - start)
}

export function InvocationsPanel({ fn }: { fn: FunctionResponse }) {
	const [limit, setLimit] = useState(50)
	const [filters, setFilters] = useState<InvocationFilters>({
		status: '',
		mode: '',
		origin: '',
		revision: '',
	})
	const query = useApi<ListResponse<InvocationResponse>>(
		apiPath`/v1/functions/${fn.id}/invocations`,
		{ limit },
		{
			refreshInterval: d =>
				d?.items.some(i => !isTerminal(i.status)) ? 2000 : 0,
		},
	)
	const revisions = useApi<ListResponse<RevisionResponse>>(
		apiPath`/v1/functions/${fn.id}/revisions`,
	)
	const knownRedrives = useKnownRedrives(fn.id)
	const revisionNumber = useMemo(() => {
		const m = new Map<string, number>()
		for (const r of revisions.data?.items ?? []) m.set(r.id, r.number)
		return m
	}, [revisions.data])

	const set =
		(k: keyof InvocationFilters) => (e: React.ChangeEvent<HTMLSelectElement>) =>
			setFilters(f => ({ ...f, [k]: e.target.value }))

	return (
		<div className='flex flex-col gap-4'>
			<div
				className='flex flex-wrap items-end gap-3'
				data-testid='invocation-filters'
			>
				<div className='flex flex-col gap-1'>
					<Label htmlFor='f-status'>Status</Label>
					<select
						id='f-status'
						data-testid='filter-status'
						className={selectClass}
						value={filters.status}
						onChange={set('status')}
					>
						<option value=''>any</option>
						{STATUSES.map(s => (
							<option key={s} value={s}>
								{s}
							</option>
						))}
					</select>
				</div>
				<div className='flex flex-col gap-1'>
					<Label htmlFor='f-mode'>Mode</Label>
					<select
						id='f-mode'
						data-testid='filter-mode'
						className={selectClass}
						value={filters.mode}
						onChange={set('mode')}
					>
						<option value=''>any</option>
						<option value='sync'>sync</option>
						<option value='async'>async</option>
					</select>
				</div>
				<div className='flex flex-col gap-1'>
					<Label htmlFor='f-origin'>Origin</Label>
					<select
						id='f-origin'
						data-testid='filter-origin'
						className={selectClass}
						value={filters.origin}
						onChange={set('origin')}
					>
						<option value=''>any</option>
						<option value='new'>new invocation</option>
						<option value='redrive'>redrive of a dead letter</option>
					</select>
				</div>
				<div className='flex flex-col gap-1'>
					<Label htmlFor='f-revision'>Revision</Label>
					<select
						id='f-revision'
						data-testid='filter-revision'
						className={selectClass}
						value={filters.revision}
						onChange={set('revision')}
					>
						<option value=''>any</option>
						{(revisions.data?.items ?? []).map(r => (
							<option key={r.id} value={r.id}>
								#{r.number}
							</option>
						))}
					</select>
				</div>
				<div className='flex flex-col gap-1'>
					<Label htmlFor='f-limit'>Latest</Label>
					<select
						id='f-limit'
						className={selectClass}
						value={limit}
						onChange={e => setLimit(Number(e.target.value))}
					>
						{[50, 200, 500].map(n => (
							<option key={n} value={n}>
								{n}
							</option>
						))}
					</select>
				</div>
				<Button
					size='sm'
					variant='outline'
					onClick={() => query.mutate()}
					data-testid='invocations-refresh'
				>
					<RefreshCw />
					Refresh
				</Button>
			</div>
			<p className='text-xs text-muted-foreground'>
				A <strong>retry</strong> is another attempt of the same invocation (the
				attempts column). A <strong>redrive</strong> or a new test invoke is a
				separate invocation with its own id. Filters apply to the latest {limit}{' '}
				invocations returned by the API.
			</p>
			<DataState
				query={query}
				resource='invocation'
				isEmpty={d => d.items.length === 0}
				empty={
					<EmptyState title='No invocations yet'>
						Run a test invoke to create one.
					</EmptyState>
				}
			>
				{data => {
					const rows = filterInvocations(data.items, filters, knownRedrives)
					if (rows.length === 0) {
						return <EmptyState title='No invocation matches these filters' />
					}
					return (
						<Table data-testid='invocations-table'>
							<TableHeader>
								<TableRow>
									<TableHead>Invocation</TableHead>
									<TableHead>Status</TableHead>
									<TableHead>Mode</TableHead>
									<TableHead>Origin</TableHead>
									<TableHead>Attempts</TableHead>
									<TableHead>Revision</TableHead>
									<TableHead>Accepted</TableHead>
									<TableHead>Duration</TableHead>
									<TableHead>Error</TableHead>
								</TableRow>
							</TableHeader>
							<TableBody>
								{rows.map(inv => {
									const origin = invocationOrigin(inv, knownRedrives)
									const retries = retryCount(inv)
									const attempts = Math.max(
										inv.attempts?.length ?? 0,
										inv.dispatch?.attempts ?? 0,
									)
									return (
										<TableRow
											key={inv.id}
											data-testid='invocation-row'
											data-invocation-id={inv.id}
											data-status={inv.status}
											data-origin={origin.kind}
										>
											<TableCell>
												<Link
													className='hover:underline'
													href={routes.invocation(inv.id)}
												>
													<Mono>{shortId(inv.id)}</Mono>
												</Link>
											</TableCell>
											<TableCell>
												<InvocationStatusBadge status={inv.status} />
											</TableCell>
											<TableCell>{inv.mode}</TableCell>
											<TableCell>
												{origin.kind === 'redrive' ? (
													<span className='flex flex-col'>
														<Badge
															variant='outline'
															data-testid='origin-redrive'
														>
															redrive
														</Badge>
														<Link
															className='text-xs hover:underline'
															href={routes.invocation(
																origin.sourceInvocationId,
															)}
														>
															of{' '}
															<Mono>{shortId(origin.sourceInvocationId)}</Mono>
														</Link>
													</span>
												) : (
													<Badge variant='secondary' data-testid='origin-new'>
														new
													</Badge>
												)}
											</TableCell>
											<TableCell data-testid='invocation-attempts'>
												{attempts}
												{retries > 0 && (
													<span className='text-xs text-muted-foreground'>
														{' '}
														({retries} {retries === 1 ? 'retry' : 'retries'})
													</span>
												)}
											</TableCell>
											<TableCell>
												<Link
													className='hover:underline'
													href={routes.revision(fn.id, inv.revision_id)}
												>
													#{revisionNumber.get(inv.revision_id) ?? '?'}
												</Link>
												{inv.alias && (
													<span className='text-xs text-muted-foreground'>
														{' '}
														via {inv.alias}
													</span>
												)}
											</TableCell>
											<TableCell className='whitespace-nowrap'>
												{formatDateTime(inv.accepted_at)}
											</TableCell>
											<TableCell>{formatMs(durationMs(inv))}</TableCell>
											<TableCell className='max-w-48 truncate text-xs'>
												{inv.error
													? `${inv.error.class} · ${inv.error.error_type}`
													: '—'}
											</TableCell>
										</TableRow>
									)
								})}
							</TableBody>
						</Table>
					)
				}}
			</DataState>
		</div>
	)
}
