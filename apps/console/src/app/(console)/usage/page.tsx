'use client'

import { DataState, EmptyState } from '@/components/functions/data-state'
import { KeyValueList, Mono, PageHeader } from '@/components/functions/kv'
import { ProvisionalBanner } from '@/components/functions/notices'
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
	Table,
	TableBody,
	TableCell,
	TableFooter,
	TableHead,
	TableHeader,
	TableRow,
} from '@/components/ui/table'
import {
	formatBytes,
	formatCount,
	formatDateTime,
	formatMicros,
	formatMs,
	shortId,
} from '@/lib/format'
import { routes } from '@/lib/navigation'
import { useApi } from '@/lib/serverless-api/hooks'
import type {
	FunctionResponse,
	ListResponse,
	UsageReportLine,
	UsageReportResponse,
} from '@/lib/serverless-api/types'
import Link from 'next/link'
import { useState } from 'react'

const selectClass =
	'flex h-10 rounded-md border border-input bg-background px-2 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'

export default function UsagePage() {
	const [from, setFrom] = useState('')
	const [to, setTo] = useState('')
	const [groupBy, setGroupBy] = useState('function')
	const [functionId, setFunctionId] = useState('')
	const functions = useApi<ListResponse<FunctionResponse>>('/v1/functions')
	const names = new Map((functions.data?.items ?? []).map(f => [f.id, f.name]))
	const query = useApi<UsageReportResponse>('/v1/usage', {
		from: from || undefined,
		to: to || undefined,
		group_by: groupBy,
		function_id: functionId || undefined,
	})

	return (
		<>
			<PageHeader
				title='Usage and provisional charges'
				description='Host-measured usage of your tenant, rated with a versioned provisional price table.'
			/>
			<ProvisionalBanner notice={query.data?.notice} />
			<div className='flex flex-wrap items-end gap-3'>
				<div className='flex flex-col gap-1'>
					<Label htmlFor='u-from'>From</Label>
					<Input
						id='u-from'
						type='date'
						value={from}
						onChange={e => setFrom(e.target.value)}
					/>
				</div>
				<div className='flex flex-col gap-1'>
					<Label htmlFor='u-to'>To (exclusive)</Label>
					<Input
						id='u-to'
						type='date'
						value={to}
						onChange={e => setTo(e.target.value)}
					/>
				</div>
				<div className='flex flex-col gap-1'>
					<Label htmlFor='u-group'>Group by</Label>
					<select
						id='u-group'
						className={selectClass}
						value={groupBy}
						onChange={e => setGroupBy(e.target.value)}
					>
						<option value='function'>function</option>
						<option value='day'>day</option>
						<option value='function,day'>function, day</option>
						<option value='none'>none</option>
					</select>
				</div>
				<div className='flex flex-col gap-1'>
					<Label htmlFor='u-fn'>Function</Label>
					<select
						id='u-fn'
						className={selectClass}
						value={functionId}
						onChange={e => setFunctionId(e.target.value)}
					>
						<option value=''>all</option>
						{(functions.data?.items ?? []).map(f => (
							<option key={f.id} value={f.id}>
								{f.name}
							</option>
						))}
					</select>
				</div>
			</div>
			<DataState query={query} resource='usage report'>
				{report => (
					<div className='flex flex-col gap-6' data-testid='usage-report'>
						<Card>
							<CardHeader>
								<CardTitle className='text-base'>Totals</CardTitle>
								<CardDescription>
									{formatDateTime(report.from)} → {formatDateTime(report.to)} ·
									collected through {formatDateTime(report.collected_through)}
								</CardDescription>
							</CardHeader>
							<CardContent>
								<KeyValueList
									items={[
										[
											'Provisional total',
											<strong key='t' data-testid='usage-total'>
												{formatMicros(
													report.totals.provisional_charges_micros.total,
													report.price_table.currency,
												)}
											</strong>,
										],
										[
											'Invocations',
											formatCount(report.totals.usage.invocations),
										],
										[
											'Attempts (retries)',
											`${formatCount(report.totals.usage.attempts)} (${formatCount(report.totals.usage.retries)})`,
										],
										[
											'Billable time',
											formatMs(report.totals.usage.billable_ms),
										],
										[
											'Unmetered attempts',
											formatCount(report.totals.unmetered.attempts),
										],
										[
											'Events not journaled',
											formatCount(report.unjournaled_events),
										],
										['Billing enabled', report.billing_enabled ? 'yes' : 'no'],
										['Not an invoice', report.not_an_invoice ? 'yes' : 'no'],
									]}
								/>
							</CardContent>
						</Card>
						<Card>
							<CardHeader>
								<CardTitle className='text-base'>Lines</CardTitle>
							</CardHeader>
							<CardContent>
								{report.lines.length === 0 ? (
									<EmptyState title='No usage in this range' />
								) : (
									<Table data-testid='usage-lines'>
										<TableHeader>
											<TableRow>
												<TableHead>Function</TableHead>
												<TableHead>Day</TableHead>
												<TableHead>Invocations</TableHead>
												<TableHead>Retries</TableHead>
												<TableHead>Billable</TableHead>
												<TableHead>Bytes in / out</TableHead>
												<TableHead>Provisional</TableHead>
											</TableRow>
										</TableHeader>
										<TableBody>
											{report.lines.map((l: UsageReportLine, i) => (
												<TableRow key={i}>
													<TableCell>
														{l.function_id ? (
															<Link
																className='hover:underline'
																href={routes.function(l.function_id, 'usage')}
															>
																{names.get(l.function_id) ?? (
																	<Mono>{shortId(l.function_id)}</Mono>
																)}
															</Link>
														) : (
															'—'
														)}
													</TableCell>
													<TableCell>{l.day ?? '—'}</TableCell>
													<TableCell>
														{formatCount(l.usage.invocations)}
													</TableCell>
													<TableCell>{formatCount(l.usage.retries)}</TableCell>
													<TableCell>{formatMs(l.usage.billable_ms)}</TableCell>
													<TableCell>
														{formatBytes(l.usage.request_bytes)} /{' '}
														{formatBytes(l.usage.response_bytes)}
													</TableCell>
													<TableCell>
														{formatMicros(
															l.provisional_charges_micros.total,
															report.price_table.currency,
														)}
													</TableCell>
												</TableRow>
											))}
										</TableBody>
										<TableFooter>
											<TableRow>
												<TableCell colSpan={6}>
													Total (provisional, not an invoice)
												</TableCell>
												<TableCell>
													{formatMicros(
														report.totals.provisional_charges_micros.total,
														report.price_table.currency,
													)}
												</TableCell>
											</TableRow>
										</TableFooter>
									</Table>
								)}
							</CardContent>
						</Card>
						<Card>
							<CardHeader>
								<CardTitle className='text-base'>
									Provisional price table
								</CardTitle>
								<CardDescription>
									Version {report.price_table.version}. Prices are placeholders
									for the prototype.
								</CardDescription>
							</CardHeader>
							<CardContent>
								<KeyValueList
									items={[
										['Currency', report.price_table.currency],
										[
											'Effective from',
											formatDateTime(report.price_table.effective_from),
										],
										[
											'Billable segments',
											report.price_table.billable_segments.join(', '),
										],
										...Object.entries(
											report.price_table.unit_prices_micros,
										).map(
											([k, v]) =>
												[
													`Unit price: ${k}`,
													formatMicros(
														v as number,
														report.price_table.currency,
													),
												] as [string, string],
										),
										['Rounding', report.price_table.rounding.join('; ')],
									]}
								/>
							</CardContent>
						</Card>
					</div>
				)}
			</DataState>
		</>
	)
}
