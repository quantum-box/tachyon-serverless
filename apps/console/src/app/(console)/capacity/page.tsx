'use client'

import { DataState, EmptyState } from '@/components/functions/data-state'
import { KeyValueList, Mono, PageHeader } from '@/components/functions/kv'
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
import { formatBytes, formatCount, formatMs, shortId } from '@/lib/format'
import { useApi } from '@/lib/serverless-api/hooks'
import type { CapacityInfo } from '@/lib/serverless-api/types'

function amounts(
	a:
		| {
				cpu_millis?: number | null
				memory_mib?: number | null
				ephemeral_storage_mib?: number | null
		  }
		| undefined,
) {
	if (!a) return '—'
	const part = (v: number | null | undefined, unit: string) =>
		v === null || v === undefined ? 'unbounded' : `${v} ${unit}`
	return `cpu ${part(a.cpu_millis, 'm')} · memory ${part(a.memory_mib, 'MiB')} · storage ${part(a.ephemeral_storage_mib, 'MiB')}`
}

export default function CapacityPage() {
	const query = useApi<CapacityInfo>('/v1/capacity', undefined, {
		refreshInterval: 5000,
	})
	return (
		<>
			<PageHeader
				title='Capacity'
				description='This gateway node: capacity, reservations, queue, and the environments of your revisions.'
			/>
			<DataState query={query} resource='capacity summary'>
				{c => (
					<div className='flex flex-col gap-6' data-testid='capacity'>
						<div className='grid gap-6 lg:grid-cols-2'>
							<Card>
								<CardHeader>
									<CardTitle className='text-base'>Node</CardTitle>
									<CardDescription>
										{c.node.name}
										{c.node.region ? ` · region ${c.node.region}` : ''} ·{' '}
										{c.node.hosts} host
									</CardDescription>
								</CardHeader>
								<CardContent>
									<KeyValueList
										items={[
											['Max concurrency', formatCount(c.node.max_concurrency)],
											['In flight', formatCount(c.in_flight)],
											['Capacity', amounts(c.node.capacity)],
											['Reserved', amounts(c.reserved)],
											[
												'Environments',
												`starting ${c.environments.starting} · busy ${c.environments.busy} · idle ${c.environments.idle} · draining ${c.environments.draining}`,
											],
											[
												'Queue',
												`${c.queue.length}/${c.queue.max_length} · ${formatBytes(c.queue.bytes)} of ${formatBytes(c.queue.max_bytes)} · oldest ${formatMs(c.queue.oldest_age_ms)}`,
											],
											[
												'Rejections',
												Object.entries(c.rejections)
													.map(([k, v]) => `${k} ${v}`)
													.join(' · ') || 'none',
											],
										]}
									/>
								</CardContent>
							</Card>
							<Card>
								<CardHeader>
									<CardTitle className='text-base'>Your tenant</CardTitle>
									<CardDescription>
										<Mono>{c.tenant.tenant_id}</Mono>
									</CardDescription>
								</CardHeader>
								<CardContent>
									<KeyValueList
										items={[
											['In flight', formatCount(c.tenant.in_flight)],
											[
												'Max concurrency',
												c.tenant.max_concurrency ?? 'node limit',
											],
											[
												'Queued',
												`${c.tenant.queued} (${formatBytes(c.tenant.queued_bytes)})`,
											],
											['Max queue', c.tenant.max_queue ?? 'node limit'],
											['Weight', c.tenant.weight],
											['Required region', c.tenant.required_region ?? '—'],
										]}
									/>
								</CardContent>
							</Card>
						</div>
						<Card>
							<CardHeader>
								<CardTitle className='text-base'>
									Revisions with environments
								</CardTitle>
							</CardHeader>
							<CardContent>
								{c.revisions.length === 0 ? (
									<EmptyState title='No environments or routes for your revisions on this node' />
								) : (
									<Table data-testid='capacity-revisions'>
										<TableHeader>
											<TableRow>
												<TableHead>Revision</TableHead>
												<TableHead>Environments</TableHead>
												<TableHead>Desired / max</TableHead>
												<TableHead>Breaker</TableHead>
												<TableHead>Arrival rate</TableHead>
												<TableHead>Avg duration</TableHead>
											</TableRow>
										</TableHeader>
										<TableBody>
											{c.revisions.map(r => {
												const row = r as typeof r & { revision_id?: string }
												return (
													<TableRow
														key={
															row.revision_id ?? JSON.stringify(r.environments)
														}
													>
														<TableCell>
															<Mono>{shortId(row.revision_id)}</Mono>
														</TableCell>
														<TableCell>
															busy {r.environments.busy} · idle{' '}
															{r.environments.idle} · starting{' '}
															{r.environments.starting}
														</TableCell>
														<TableCell>
															{r.desired} / {r.max_environments}
														</TableCell>
														<TableCell>{r.circuit_breaker ?? '—'}</TableCell>
														<TableCell>
															{r.arrival_rate_per_second.toFixed(2)}/s
														</TableCell>
														<TableCell>{formatMs(r.avg_duration_ms)}</TableCell>
													</TableRow>
												)
											})}
										</TableBody>
									</Table>
								)}
							</CardContent>
						</Card>
					</div>
				)}
			</DataState>
		</>
	)
}
