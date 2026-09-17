'use client'

import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from '@/components/ui/card'
import { Label } from '@/components/ui/label'
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
import {
	type InvocationResponse,
	type LogsResponse,
	isTerminal,
} from '@/lib/serverless-api/types'
import Link from 'next/link'
import { useRouter } from 'next/navigation'
import { useState } from 'react'
import { DataState, EmptyState } from './data-state'

const selectClass =
	'flex h-9 rounded-md border border-input bg-background px-2 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'

export function InvocationLogs({
	inv,
	attemptFilter,
}: { inv: InvocationResponse; attemptFilter: string }) {
	const router = useRouter()
	const [stream, setStream] = useState('')
	const query = useApi<LogsResponse>(
		apiPath`/v1/invocations/${inv.id}/logs`,
		undefined,
		{
			refreshInterval: isTerminal(inv.status) ? 0 : 2000,
		},
	)
	const attempts = inv.attempts ?? []

	return (
		<Card>
			<CardHeader>
				<CardTitle className='text-base'>Logs</CardTitle>
				<CardDescription>
					stdout / stderr of the handler and platform lines of the environment,
					capped per invocation. Lines are shown as the API returns them.
				</CardDescription>
			</CardHeader>
			<CardContent className='flex flex-col gap-3'>
				<div className='flex flex-wrap items-end gap-3'>
					<div className='flex flex-col gap-1'>
						<Label htmlFor='log-attempt'>Attempt</Label>
						<select
							id='log-attempt'
							data-testid='log-attempt-filter'
							className={selectClass}
							value={attemptFilter}
							onChange={e =>
								router.replace(
									routes.invocationLogs(inv.id, e.target.value || undefined),
									{ scroll: false },
								)
							}
						>
							<option value=''>all</option>
							{attempts.map(a => (
								<option key={a.id} value={a.id}>
									attempt {a.number}
								</option>
							))}
						</select>
					</div>
					<div className='flex flex-col gap-1'>
						<Label htmlFor='log-stream'>Stream</Label>
						<select
							id='log-stream'
							className={selectClass}
							value={stream}
							onChange={e => setStream(e.target.value)}
						>
							<option value=''>all</option>
							<option value='stdout'>stdout</option>
							<option value='stderr'>stderr</option>
							<option value='platform'>platform</option>
						</select>
					</div>
				</div>
				<DataState
					query={query}
					resource='log'
					isEmpty={d => d.items.length === 0}
					empty={<EmptyState title='No log lines' />}
				>
					{data => {
						const lines = data.items.filter(
							l =>
								(stream === '' || l.stream === stream) &&
								// Environment lines (boot / init) carry no attempt id and stay visible.
								(attemptFilter === '' ||
									!l.attempt_id ||
									l.attempt_id === attemptFilter),
						)
						return (
							<>
								{data.dropped && (
									<p
										className='text-xs text-destructive'
										data-testid='logs-dropped'
									>
										Some lines were dropped: the per-invocation line / byte cap
										was reached.
									</p>
								)}
								<Table data-testid='logs-table'>
									<TableHeader>
										<TableRow>
											<TableHead>Time</TableHead>
											<TableHead>Stream</TableHead>
											<TableHead>Phase</TableHead>
											<TableHead>Attempt</TableHead>
											<TableHead>Line</TableHead>
										</TableRow>
									</TableHeader>
									<TableBody>
										{lines.map((l, i) => (
											<TableRow
												key={i}
												data-testid='log-line'
												data-attempt-id={l.attempt_id ?? ''}
											>
												<TableCell className='whitespace-nowrap font-mono text-xs'>
													{formatDateTime(l.timestamp)}
												</TableCell>
												<TableCell>{l.stream}</TableCell>
												<TableCell>{l.phase}</TableCell>
												<TableCell className='font-mono text-xs'>
													{l.attempt_id ? (
														<Link
															className='hover:underline'
															href={routes.invocation(inv.id, l.attempt_id)}
														>
															{shortId(l.attempt_id)}
														</Link>
													) : (
														'—'
													)}
												</TableCell>
												<TableCell className='whitespace-pre-wrap break-all font-mono text-xs'>
													{l.line}
													{l.truncated && (
														<span className='text-muted-foreground'>
															{' '}
															[truncated]
														</span>
													)}
												</TableCell>
											</TableRow>
										))}
									</TableBody>
								</Table>
							</>
						)
					}}
				</DataState>
			</CardContent>
		</Card>
	)
}
