'use client'

import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from '@/components/ui/table'
import { formatDateTime, shortId } from '@/lib/format'
import { routes } from '@/lib/navigation'
import { apiPath } from '@/lib/serverless-api/client'
import { useApi } from '@/lib/serverless-api/hooks'
import type {
	DeadLetterResponse,
	FunctionResponse,
	ListResponse,
} from '@/lib/serverless-api/types'
import Link from 'next/link'
import { DataState, EmptyState } from './data-state'
import { Mono } from './kv'
import { DeadLetterStatusBadge } from './status-badge'

export function DeadLettersPanel({ fn }: { fn: FunctionResponse }) {
	const query = useApi<ListResponse<DeadLetterResponse>>(
		apiPath`/v1/functions/${fn.id}/dead-letters`,
		{
			limit: 100,
		},
	)
	return (
		<div className='flex flex-col gap-3'>
			<p className='text-xs text-muted-foreground'>
				Asynchronous invocations that ran out of attempts, expired or could not
				be retried. Redriving one creates a <strong>new</strong> asynchronous
				invocation with the same stored input; the original invocation stays as
				it ended.
			</p>
			<DataState
				query={query}
				resource='dead letter'
				isEmpty={d => d.items.length === 0}
				empty={<EmptyState title='No dead letters' />}
			>
				{data => (
					<Table data-testid='dead-letters-table'>
						<TableHeader>
							<TableRow>
								<TableHead>Dead letter</TableHead>
								<TableHead>Status</TableHead>
								<TableHead>Reason</TableHead>
								<TableHead>Invocation</TableHead>
								<TableHead>Attempts</TableHead>
								<TableHead>Last error</TableHead>
								<TableHead>Redrives</TableHead>
								<TableHead>Created</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{data.items.map(d => (
								<TableRow
									key={d.id}
									data-testid='dead-letter-row'
									data-dead-letter-id={d.id}
									data-status={d.status}
								>
									<TableCell>
										<Link
											className='hover:underline'
											href={routes.deadLetter(d.id)}
										>
											<Mono>{shortId(d.id)}</Mono>
										</Link>
									</TableCell>
									<TableCell>
										<DeadLetterStatusBadge status={d.status} />
									</TableCell>
									<TableCell>{d.reason}</TableCell>
									<TableCell>
										{d.invocation_id ? (
											<Link
												className='hover:underline'
												href={routes.invocation(d.invocation_id)}
											>
												<Mono>{shortId(d.invocation_id)}</Mono>
											</Link>
										) : (
											'—'
										)}
									</TableCell>
									<TableCell>{d.attempts}</TableCell>
									<TableCell className='max-w-48 truncate text-xs'>
										{d.last_error
											? `${d.last_error.class} · ${d.last_error.error_type}`
											: '—'}
									</TableCell>
									<TableCell>{d.redrive_count}</TableCell>
									<TableCell className='whitespace-nowrap'>
										{formatDateTime(d.created_at)}
									</TableCell>
								</TableRow>
							))}
						</TableBody>
					</Table>
				)}
			</DataState>
		</div>
	)
}
