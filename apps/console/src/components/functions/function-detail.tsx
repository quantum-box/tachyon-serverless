'use client'

import { Badge } from '@/components/ui/badge'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { formatDateTime } from '@/lib/format'
import { type FunctionTab, parseFunctionTab, routes } from '@/lib/navigation'
import { ApiError, apiPath } from '@/lib/serverless-api/client'
import { useApi } from '@/lib/serverless-api/hooks'
import type { FunctionResponse } from '@/lib/serverless-api/types'
import Link from 'next/link'
import { useRouter, useSearchParams } from 'next/navigation'
import { DataState, ErrorState } from './data-state'
import { DeadLettersPanel } from './dead-letters-panel'
import { FunctionUsagePanel } from './function-usage-panel'
import { InvocationsPanel } from './invocations-panel'
import { InvokePanel } from './invoke-panel'
import { Mono, PageHeader } from './kv'
import { RevisionsPanel } from './revisions-panel'

export function FunctionDetail() {
	const search = useSearchParams()
	const router = useRouter()
	const id = search.get('id') ?? ''
	const tab = parseFunctionTab(search.get('tab'))
	const query = useApi<FunctionResponse>(
		id ? apiPath`/v1/functions/${id}` : null,
	)

	if (!id) {
		return (
			<ErrorState
				resource='function'
				error={
					new ApiError({
						status: 404,
						code: 'not_found',
						message: 'no function id in the URL',
					})
				}
			/>
		)
	}

	return (
		<DataState query={query} resource='function'>
			{fn => (
				<>
					<PageHeader
						breadcrumbs={
							<>
								<Link href={routes.functions} className='hover:underline'>
									Functions
								</Link>{' '}
								/ {fn.name}
							</>
						}
						title={<span data-testid='function-name'>{fn.name}</span>}
						description={
							<span className='flex flex-wrap items-center gap-2'>
								<Mono>{fn.id}</Mono>
								<Badge
									variant={
										fn.deletion_state && fn.deletion_state !== 'live'
											? 'destructive'
											: 'secondary'
									}
								>
									{fn.deletion_state ?? 'live'}
								</Badge>
								<span>created {formatDateTime(fn.created_at)}</span>
							</span>
						}
					/>
					<Tabs
						value={tab}
						onValueChange={v =>
							router.replace(routes.function(fn.id, v as FunctionTab), {
								scroll: false,
							})
						}
					>
						<TabsList>
							<TabsTrigger value='overview'>Deployments</TabsTrigger>
							<TabsTrigger value='invoke'>Test invoke</TabsTrigger>
							<TabsTrigger value='invocations'>Invocations</TabsTrigger>
							<TabsTrigger value='dead-letters'>Dead letters</TabsTrigger>
							<TabsTrigger value='usage'>Usage</TabsTrigger>
						</TabsList>
						<TabsContent value='overview'>
							<RevisionsPanel fn={fn} />
						</TabsContent>
						<TabsContent value='invoke'>
							<InvokePanel fn={fn} />
						</TabsContent>
						<TabsContent value='invocations'>
							<InvocationsPanel fn={fn} />
						</TabsContent>
						<TabsContent value='dead-letters'>
							<DeadLettersPanel fn={fn} />
						</TabsContent>
						<TabsContent value='usage'>
							<FunctionUsagePanel fn={fn} />
						</TabsContent>
					</Tabs>
				</>
			)}
		</DataState>
	)
}
