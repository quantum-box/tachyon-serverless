'use client'

import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from '@/components/ui/card'
import { formatBytes, formatCount, formatMs } from '@/lib/format'
import { routes } from '@/lib/navigation'
import { apiPath } from '@/lib/serverless-api/client'
import { useApi } from '@/lib/serverless-api/hooks'
import type {
	FunctionResponse,
	UsageSummaryResponse,
} from '@/lib/serverless-api/types'
import Link from 'next/link'
import { DataState } from './data-state'
import { KeyValueList } from './kv'

export function FunctionUsagePanel({ fn }: { fn: FunctionResponse }) {
	const query = useApi<UsageSummaryResponse>(
		apiPath`/v1/functions/${fn.id}/usage`,
	)
	return (
		<Card>
			<CardHeader>
				<CardTitle className='text-base'>Usage facts</CardTitle>
				<CardDescription>
					Host-measured totals of this function. These are usage facts, not
					charges (<code>not_billable</code> is always true). Provisional
					charges are on the{' '}
					<Link className='underline' href={routes.usage}>
						Usage
					</Link>{' '}
					page.
				</CardDescription>
			</CardHeader>
			<CardContent>
				<DataState query={query} resource='usage summary'>
					{u => (
						<div data-testid='function-usage'>
							<KeyValueList
								items={[
									['Invocations', formatCount(u.invocations)],
									['Succeeded', formatCount(u.succeeded)],
									['Failed', formatCount(u.failed)],
									['Handler time', formatMs(u.handler_ms_total)],
									['Environment time', formatMs(u.environment_ms_total)],
									['Bytes in', formatBytes(u.bytes_in_total)],
									['Bytes out', formatBytes(u.bytes_out_total)],
									[
										'Billable',
										u.not_billable ? 'no (usage facts only)' : 'yes',
									],
								]}
							/>
						</div>
					)}
				</DataState>
			</CardContent>
		</Card>
	)
}
