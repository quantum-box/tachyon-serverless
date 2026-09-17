'use client'

import { DataState, EmptyState } from '@/components/functions/data-state'
import { Mono, PageHeader } from '@/components/functions/kv'
import { Badge } from '@/components/ui/badge'
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from '@/components/ui/table'
import { formatDateTime } from '@/lib/format'
import { routes } from '@/lib/navigation'
import { useApi } from '@/lib/serverless-api/hooks'
import type { FunctionResponse, ListResponse } from '@/lib/serverless-api/types'
import Link from 'next/link'

export default function FunctionsPage() {
	const query = useApi<ListResponse<FunctionResponse>>('/v1/functions')
	return (
		<>
			<PageHeader
				title='Functions'
				description='Functions of your tenant. Deploy with the tsls CLI; this console reads the management API and runs test invokes, rollbacks, cancels and redrives.'
			/>
			<DataState
				query={query}
				resource='function'
				isEmpty={d => d.items.length === 0}
				empty={
					<EmptyState title='No functions yet'>
						<p>
							Create and deploy one with <code>tsls functions create</code> and{' '}
							<code>tsls functions deploy</code> (docs/cli.md).
						</p>
					</EmptyState>
				}
			>
				{data => (
					<Table data-testid='functions-table'>
						<TableHeader>
							<TableRow>
								<TableHead>Name</TableHead>
								<TableHead>Id</TableHead>
								<TableHead>State</TableHead>
								<TableHead>Description</TableHead>
								<TableHead>Updated</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{data.items.map(fn => (
								<TableRow
									key={fn.id}
									data-testid='function-row'
									data-function-id={fn.id}
								>
									<TableCell>
										<Link
											className='font-medium underline-offset-4 hover:underline'
											href={routes.function(fn.id)}
										>
											{fn.name}
										</Link>
									</TableCell>
									<TableCell>
										<Mono>{fn.id}</Mono>
									</TableCell>
									<TableCell>
										<Badge
											variant={
												fn.deletion_state && fn.deletion_state !== 'live'
													? 'destructive'
													: 'secondary'
											}
										>
											{fn.deletion_state ?? 'live'}
										</Badge>
									</TableCell>
									<TableCell className='max-w-xs truncate'>
										{fn.description || '—'}
									</TableCell>
									<TableCell>{formatDateTime(fn.updated_at)}</TableCell>
								</TableRow>
							))}
						</TableBody>
					</Table>
				)}
			</DataState>
		</>
	)
}
