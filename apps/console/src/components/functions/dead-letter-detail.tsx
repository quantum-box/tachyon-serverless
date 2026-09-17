'use client'

import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from '@/components/ui/card'
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from '@/components/ui/table'
import { formatBytes, formatDateTime, shortId } from '@/lib/format'
import { routes } from '@/lib/navigation'
import { notifyFailure, notifySuccess } from '@/lib/notify'
import { ApiError, apiPath, apiRequest } from '@/lib/serverless-api/client'
import { useApi } from '@/lib/serverless-api/hooks'
import type {
	DeadLetterResponse,
	RedriveAcceptedResponse,
} from '@/lib/serverless-api/types'
import { useCredentials } from '@/lib/session'
import { RotateCcw } from 'lucide-react'
import Link from 'next/link'
import { useSearchParams } from 'next/navigation'
import { useState } from 'react'
import { ConfirmAction } from './confirm-action'
import { DataState, ErrorState } from './data-state'
import { KeyValueList, Mono, PageHeader } from './kv'
import { DeadLetterStatusBadge } from './status-badge'

export function DeadLetterDetail() {
	const search = useSearchParams()
	const id = search.get('id') ?? ''
	const query = useApi<DeadLetterResponse>(
		id ? apiPath`/v1/dead-letters/${id}` : null,
	)
	if (!id) {
		return (
			<ErrorState
				resource='dead letter'
				error={
					new ApiError({
						status: 404,
						code: 'not_found',
						message: 'no dead letter id in the URL',
					})
				}
			/>
		)
	}
	return (
		<DataState query={query} resource='dead letter'>
			{d => <DeadLetterView d={d} onChanged={() => query.mutate()} />}
		</DataState>
	)
}

function DeadLetterView({
	d,
	onChanged,
}: { d: DeadLetterResponse; onChanged: () => void }) {
	const credentials = useCredentials()
	const [lastRedrive, setLastRedrive] =
		useState<RedriveAcceptedResponse | null>(null)
	const canRedrive = d.status === 'open' && d.reason !== 'poison'

	return (
		<div
			className='flex flex-col gap-6'
			data-testid='dead-letter-detail'
			data-status={d.status}
		>
			<PageHeader
				breadcrumbs={
					<>
						<Link href={routes.functions} className='hover:underline'>
							Functions
						</Link>
						{d.function_id && (
							<>
								{' '}
								/{' '}
								<Link
									href={routes.function(d.function_id, 'dead-letters')}
									className='hover:underline'
								>
									dead letters
								</Link>
							</>
						)}
					</>
				}
				title={<Mono>{d.id}</Mono>}
				description={
					<span className='mt-1 flex flex-wrap items-center gap-2'>
						<DeadLetterStatusBadge status={d.status} />
						<span>reason {d.reason}</span>
					</span>
				}
				actions={
					<ConfirmAction
						testId='redrive'
						triggerVariant='default'
						triggerDisabled={!canRedrive}
						trigger={
							<>
								<RotateCcw />
								Redrive
							</>
						}
						title='Redrive this dead letter?'
						description={
							<>
								<p>
									A <strong>new asynchronous invocation</strong> is created with
									the same stored input ({formatBytes(d.input_size_bytes)},
									digest <code>{d.input_digest ?? '—'}</code>) on the original
									revision <code>{d.revision_id ?? '—'}</code>. The original
									invocation <code>{d.invocation_id ?? '—'}</code> stays as it
									ended.
								</p>
								<p>
									The handler runs again and its side effects may happen again.
									A dead letter can be redriven once. Your token needs the{' '}
									<code>invoke</code> and <code>redrive</code> roles.
								</p>
							</>
						}
						reasonLabel='Reason (recorded with the redrive)'
						confirmLabel='Redrive'
						onConfirm={async reason => {
							try {
								const res = await apiRequest<RedriveAcceptedResponse>(
									credentials,
									apiPath`/v1/dead-letters/${d.id}/redrive`,
									{ method: 'POST', body: { reason: reason || null } },
								)
								setLastRedrive(res.data)
								notifySuccess(
									'Redrive accepted',
									`New invocation ${res.data.invocation.invocation_id}`,
								)
							} catch (e) {
								notifyFailure('Redrive failed', e)
							} finally {
								onChanged()
							}
						}}
					/>
				}
			/>

			{lastRedrive && (
				<p className='text-sm' data-testid='redrive-result'>
					Redrive accepted: new invocation{' '}
					<Link
						className='underline'
						href={routes.invocation(lastRedrive.invocation.invocation_id)}
					>
						<Mono>{lastRedrive.invocation.invocation_id}</Mono>
					</Link>{' '}
					({lastRedrive.invocation.status}).
				</p>
			)}

			<Card>
				<CardHeader>
					<CardTitle className='text-base'>Details</CardTitle>
					<CardDescription>
						The input body is not shown; only its digest and size.
					</CardDescription>
				</CardHeader>
				<CardContent>
					<KeyValueList
						items={[
							[
								'Invocation',
								d.invocation_id ? (
									<Link
										key='i'
										className='underline'
										href={routes.invocation(d.invocation_id)}
									>
										<Mono>{d.invocation_id}</Mono>
									</Link>
								) : (
									'— (poison message)'
								),
							],
							[
								'Revision',
								d.function_id && d.revision_id ? (
									<Link
										key='r'
										className='underline'
										href={routes.revision(d.function_id, d.revision_id)}
									>
										<Mono>{d.revision_id}</Mono>
									</Link>
								) : (
									'—'
								),
							],
							['Counted attempts', d.attempts],
							['Deferrals', d.deferrals],
							[
								'Last error',
								d.last_error
									? `${d.last_error.class} · ${d.last_error.error_type}: ${d.last_error.message}`
									: '—',
							],
							['Detail', d.detail ?? '—'],
							['Accepted', formatDateTime(d.accepted_at)],
							['First attempt', formatDateTime(d.first_attempt_at)],
							['Last attempt', formatDateTime(d.last_attempt_at)],
							['Dead-lettered', formatDateTime(d.created_at)],
							['Input digest', <Mono key='dg'>{d.input_digest ?? '—'}</Mono>],
							['Input size', formatBytes(d.input_size_bytes)],
							['Input storage', d.input_storage ?? '—'],
						]}
					/>
				</CardContent>
			</Card>

			<Card>
				<CardHeader>
					<CardTitle className='text-base'>Redrives</CardTitle>
				</CardHeader>
				<CardContent>
					{(d.redrives ?? []).length === 0 ? (
						<p className='text-sm text-muted-foreground'>Not redriven.</p>
					) : (
						<Table data-testid='redrives-table'>
							<TableHeader>
								<TableRow>
									<TableHead>New invocation</TableHead>
									<TableHead>Requested by</TableHead>
									<TableHead>Reason</TableHead>
									<TableHead>Revision</TableHead>
									<TableHead>At</TableHead>
								</TableRow>
							</TableHeader>
							<TableBody>
								{(d.redrives ?? []).map(r => (
									<TableRow key={r.id} data-testid='redrive-row'>
										<TableCell>
											<Link
												className='hover:underline'
												href={routes.invocation(r.invocation_id)}
											>
												<Mono>{shortId(r.invocation_id)}</Mono>
											</Link>
										</TableCell>
										<TableCell>{r.requested_by}</TableCell>
										<TableCell data-testid='redrive-row-reason'>
											{r.reason || '—'}
										</TableCell>
										<TableCell>
											<Mono>{shortId(r.revision_id)}</Mono>
											{r.revision_overridden ? ' (overridden)' : ''}
										</TableCell>
										<TableCell>{formatDateTime(r.created_at)}</TableCell>
									</TableRow>
								))}
							</TableBody>
						</Table>
					)}
				</CardContent>
			</Card>
		</div>
	)
}
