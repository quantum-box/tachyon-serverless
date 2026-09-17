'use client'

import { DataState } from '@/components/functions/data-state'
import { KeyValueList, Mono, PageHeader } from '@/components/functions/kv'
import { ProvisionalBanner } from '@/components/functions/notices'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
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
	TableHead,
	TableHeader,
	TableRow,
} from '@/components/ui/table'
import {
	formatCount,
	formatDateTime,
	formatMicros,
	shortId,
} from '@/lib/format'
import { routes } from '@/lib/navigation'
import { useApi } from '@/lib/serverless-api/hooks'
import type {
	BudgetReportResponse,
	BudgetScopeReport,
	FunctionResponse,
	ListResponse,
} from '@/lib/serverless-api/types'
import { OctagonX } from 'lucide-react'
import Link from 'next/link'
import { useState } from 'react'

export default function BudgetPage() {
	const [period, setPeriod] = useState('')
	const query = useApi<BudgetReportResponse>('/v1/budget', {
		period: period || undefined,
	})
	const functions = useApi<ListResponse<FunctionResponse>>('/v1/functions')
	const names = new Map((functions.data?.items ?? []).map(f => [f.id, f.name]))

	return (
		<>
			<PageHeader
				title='Budget'
				description='Budget limits, reservations and alerts of your tenant for one UTC calendar month.'
			/>
			<ProvisionalBanner notice={query.data?.notice} />
			<div className='flex flex-wrap items-end gap-3'>
				<div className='flex flex-col gap-1'>
					<Label htmlFor='b-period'>Period</Label>
					<Input
						id='b-period'
						type='month'
						value={period}
						onChange={e => setPeriod(e.target.value)}
					/>
				</div>
			</div>
			<DataState query={query} resource='budget'>
				{b => (
					<div className='flex flex-col gap-6' data-testid='budget'>
						{!b.enabled && (
							<Alert data-testid='budget-not-enforced'>
								<AlertTitle>
									Budgets are not enforced on this gateway
								</AlertTitle>
								<AlertDescription>
									<code>[budget] enabled</code> is off: the amounts below are
									reported but no invocation is refused for budget reasons.
								</AlertDescription>
							</Alert>
						)}
						{b.enabled && !b.admitting && (
							<Alert variant='destructive' data-testid='budget-refusing'>
								<OctagonX className='size-4' />
								<AlertTitle>New invocations are refused</AlertTitle>
								<AlertDescription>
									The budget does not admit new invocations of this tenant (
									<code>{b.refusal ?? 'unknown'}</code>). Running invocations
									are not stopped.
								</AlertDescription>
							</Alert>
						)}
						<Card>
							<CardHeader>
								<CardTitle className='flex items-center gap-2 text-base'>
									Tenant budget
									<Badge
										variant={b.admitting ? 'secondary' : 'destructive'}
										data-testid='budget-admitting'
										data-admitting={String(b.admitting)}
									>
										{b.admitting ? 'admitting' : 'refusing'}
									</Badge>
								</CardTitle>
								<CardDescription>
									{b.period} ({formatDateTime(b.period_start)} →{' '}
									{formatDateTime(b.period_end)}) · price table{' '}
									{b.price_table_version} · configuration {b.config_state}
									{b.config_generation !== null &&
									b.config_generation !== undefined
										? ` (generation ${b.config_generation})`
										: ''}
								</CardDescription>
							</CardHeader>
							<CardContent>
								<ScopeFacts scope={b.tenant} currency={b.currency} />
							</CardContent>
						</Card>
						<Card>
							<CardHeader>
								<CardTitle className='text-base'>Functions</CardTitle>
								<CardDescription>
									Functions with their own budget or with reservations in the
									period.
								</CardDescription>
							</CardHeader>
							<CardContent>
								{b.functions.length === 0 ? (
									<p className='text-sm text-muted-foreground'>
										No function budget or reservation in this period.
									</p>
								) : (
									<Table data-testid='budget-functions'>
										<TableHeader>
											<TableRow>
												<TableHead>Function</TableHead>
												<TableHead>Hard limit</TableHead>
												<TableHead>Committed</TableHead>
												<TableHead>Remaining</TableHead>
												<TableHead>Settled</TableHead>
												<TableHead>Refusals</TableHead>
											</TableRow>
										</TableHeader>
										<TableBody>
											{b.functions.map((f, i) => (
												<TableRow key={f.function_id ?? i}>
													<TableCell>
														{f.function_id ? (
															<Link
																className='hover:underline'
																href={routes.function(f.function_id)}
															>
																{names.get(f.function_id) ?? (
																	<Mono>{shortId(f.function_id)}</Mono>
																)}
															</Link>
														) : (
															'—'
														)}
													</TableCell>
													<TableCell>
														{f.hard_limit_micros === null ||
														f.hard_limit_micros === undefined
															? 'none'
															: formatMicros(f.hard_limit_micros, b.currency)}
													</TableCell>
													<TableCell>
														{formatMicros(f.committed_micros, b.currency)}
													</TableCell>
													<TableCell>
														{f.remaining_micros === null ||
														f.remaining_micros === undefined
															? '—'
															: formatMicros(f.remaining_micros, b.currency)}
													</TableCell>
													<TableCell>
														{formatMicros(f.settled_micros, b.currency)}
													</TableCell>
													<TableCell>{formatCount(f.refusals)}</TableCell>
												</TableRow>
											))}
										</TableBody>
									</Table>
								)}
							</CardContent>
						</Card>
						<Card>
							<CardHeader>
								<CardTitle className='text-base'>
									What these amounts guarantee
								</CardTitle>
							</CardHeader>
							<CardContent>
								<ul
									className='list-disc pl-5 text-sm'
									data-testid='budget-guarantee'
								>
									{b.guarantee.map((g, i) => (
										<li key={i}>{g}</li>
									))}
								</ul>
							</CardContent>
						</Card>
					</div>
				)}
			</DataState>
		</>
	)
}

function ScopeFacts({
	scope,
	currency,
}: { scope: BudgetScopeReport; currency: string }) {
	const money = (v: number | null | undefined) =>
		v === null || v === undefined ? 'none' : formatMicros(v, currency)
	return (
		<div className='flex flex-col gap-4'>
			<KeyValueList
				items={[
					[
						'Hard limit (stops new invocations)',
						<span key='h' data-testid='budget-hard-limit'>
							{money(scope.hard_limit_micros)}
						</span>,
					],
					[
						'Committed (reserved + settled + unmetered hold)',
						money(scope.committed_micros),
					],
					[
						'Remaining',
						<span key='r' data-testid='budget-remaining'>
							{money(scope.remaining_micros)}
						</span>,
					],
					['Reserved by running invocations', money(scope.reserved_micros)],
					['Settled (provisional charges)', money(scope.settled_micros)],
					['Unmetered hold (not a charge)', money(scope.unmetered_hold_micros)],
					['Overrun above reservations', money(scope.overrun_micros)],
					[
						'Soft limit (alerts only)',
						`${money(scope.soft_limit_micros)} at ${
							scope.alert_thresholds_percent.length
								? scope.alert_thresholds_percent.map(p => `${p}%`).join(', ')
								: '—'
						}`,
					],
					[
						'Reservations',
						`${formatCount(scope.reservations)} (active ${formatCount(scope.active_reservations)}, settled ${formatCount(scope.settlements)}, released ${formatCount(scope.releases)}, expired ${formatCount(scope.expiries)}, refused ${formatCount(scope.refusals)})`,
					],
				]}
			/>
			<div>
				<p className='mb-1 text-sm font-medium'>Alerts fired</p>
				{scope.alerts_fired.length === 0 ? (
					<p className='text-sm text-muted-foreground'>None.</p>
				) : (
					<ul className='text-sm' data-testid='budget-alerts'>
						{scope.alerts_fired.map((a, i) => (
							<li key={i}>
								{a.threshold_percent}% of {money(a.soft_limit_micros)} reached
								at {formatDateTime(a.fired_at)} (consumed{' '}
								{money(a.consumed_micros)})
							</li>
						))}
					</ul>
				)}
			</div>
		</div>
	)
}
