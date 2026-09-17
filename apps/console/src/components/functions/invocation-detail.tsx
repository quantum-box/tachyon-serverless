'use client'

import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
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
import { formatBytes, formatDateTime, formatMs } from '@/lib/format'
import { attemptKind, invocationOrigin } from '@/lib/invocation-kind'
import { routes } from '@/lib/navigation'
import { notifyFailure, notifySuccess } from '@/lib/notify'
import { ApiError, apiPath, apiRequest } from '@/lib/serverless-api/client'
import { useApi } from '@/lib/serverless-api/hooks'
import {
	type AttemptResponse,
	type FunctionResponse,
	type InvocationResponse,
	type ListResponse,
	type RevisionResponse,
	isTerminal,
} from '@/lib/serverless-api/types'
import { useCredentials } from '@/lib/session'
import { Ban, Eye, EyeOff, ShieldQuestion } from 'lucide-react'
import Link from 'next/link'
import { useSearchParams } from 'next/navigation'
import { useState } from 'react'
import { ConfirmAction } from './confirm-action'
import { DataState, ErrorState } from './data-state'
import { JsonBlock, KeyValueList, Mono, PageHeader } from './kv'
import { InvocationLogs } from './invocation-logs'
import { OutcomeUnknownNotice } from './notices'
import { InvocationStatusBadge } from './status-badge'

export function InvocationDetail() {
	const search = useSearchParams()
	const id = search.get('id') ?? ''
	const attemptFilter = search.get('attempt') ?? ''
	const query = useApi<InvocationResponse>(
		id ? apiPath`/v1/invocations/${id}` : null,
		undefined,
		{
			refreshInterval: d => (d && !isTerminal(d.status) ? 1500 : 0),
		},
	)
	if (!id) {
		return (
			<ErrorState
				resource='invocation'
				error={
					new ApiError({
						status: 404,
						code: 'not_found',
						message: 'no invocation id in the URL',
					})
				}
			/>
		)
	}
	return (
		<DataState query={query} resource='invocation'>
			{inv => (
				<InvocationView
					inv={inv}
					attemptFilter={attemptFilter}
					onChanged={() => query.mutate()}
				/>
			)}
		</DataState>
	)
}

function InvocationView({
	inv,
	attemptFilter,
	onChanged,
}: {
	inv: InvocationResponse
	attemptFilter: string
	onChanged: () => void
}) {
	const credentials = useCredentials()
	const fn = useApi<FunctionResponse>(apiPath`/v1/functions/${inv.function_id}`)
	const revisions = useApi<ListResponse<RevisionResponse>>(
		apiPath`/v1/functions/${inv.function_id}/revisions`,
	)
	const revisionNumber = revisions.data?.items.find(
		r => r.id === inv.revision_id,
	)?.number
	const origin = invocationOrigin(inv)
	const [inputRevealRequested, setInputRevealRequested] = useState(false)
	const [showOutput, setShowOutput] = useState(false)
	const outputText =
		inv.output === undefined ? null : JSON.stringify(inv.output)

	return (
		<div
			className='flex flex-col gap-6'
			data-testid='invocation-detail'
			data-invocation-id={inv.id}
		>
			<PageHeader
				breadcrumbs={
					<>
						<Link href={routes.functions} className='hover:underline'>
							Functions
						</Link>{' '}
						/{' '}
						<Link
							href={routes.function(inv.function_id, 'invocations')}
							className='hover:underline'
						>
							{fn.data?.name ?? inv.function_id}
						</Link>{' '}
						/ invocation
					</>
				}
				title={<Mono testId='invocation-id'>{inv.id}</Mono>}
				description={
					<span className='mt-1 flex flex-wrap items-center gap-2'>
						<InvocationStatusBadge status={inv.status} />
						<Badge variant='outline'>{inv.mode}</Badge>
						{origin.kind === 'redrive' ? (
							<Badge variant='outline' data-testid='origin-redrive'>
								redrive (new invocation)
							</Badge>
						) : (
							<Badge variant='secondary' data-testid='origin-new'>
								new invocation
							</Badge>
						)}
						<Link
							className='underline'
							href={routes.revision(inv.function_id, inv.revision_id)}
							data-testid='invocation-revision-link'
						>
							revision #{revisionNumber ?? '?'}
						</Link>
						{inv.alias && <span>via alias {inv.alias}</span>}
					</span>
				}
				actions={
					!isTerminal(inv.status) && (
						<ConfirmAction
							testId='cancel'
							triggerVariant='destructive'
							trigger={
								<>
									<Ban />
									Cancel invocation
								</>
							}
							title='Cancel this invocation?'
							description={
								<>
									<p>
										The handler of <code>{inv.id}</code> is asked to stop and
										its environment is terminated after the grace period. Work
										the handler already did is not undone.
									</p>
									<p>The invocation ends as cancelled and is not retried.</p>
								</>
							}
							confirmLabel='Cancel invocation'
							onConfirm={async () => {
								try {
									const res = await apiRequest<InvocationResponse>(
										credentials,
										apiPath`/v1/invocations/${inv.id}/cancel`,
										{ method: 'POST' },
									)
									notifySuccess(
										'Invocation cancelled',
										`Status is now ${res.data.status}.`,
									)
								} catch (e) {
									notifyFailure('Cancel failed', e)
								} finally {
									onChanged()
								}
							}}
						/>
					)
				}
			/>

			{inv.status === 'outcome_unknown' && (
				<OutcomeUnknownNotice mode={inv.mode} />
			)}

			{inv.error && (
				<Alert variant='destructive' data-testid='invocation-error'>
					<AlertTitle>
						{inv.error.class} · {inv.error.error_type}
					</AlertTitle>
					<AlertDescription>{inv.error.message}</AlertDescription>
				</Alert>
			)}

			{origin.kind === 'redrive' && (
				<Card data-testid='redrive-origin'>
					<CardHeader>
						<CardTitle className='text-base'>Created by a redrive</CardTitle>
						<CardDescription>
							This is a new invocation. The source invocation stays as it ended.
						</CardDescription>
					</CardHeader>
					<CardContent>
						<KeyValueList
							items={[
								[
									'Source invocation',
									<Link
										key='s'
										className='underline'
										href={routes.invocation(origin.sourceInvocationId)}
									>
										<Mono>{origin.sourceInvocationId}</Mono>
									</Link>,
								],
								[
									'Dead letter',
									<Link
										key='d'
										className='underline'
										href={routes.deadLetter(origin.deadLetterId)}
									>
										<Mono>{origin.deadLetterId}</Mono>
									</Link>,
								],
								['Requested by', origin.requestedBy],
								['Reason', origin.reason || '—'],
								[
									'Revision overridden',
									origin.revisionOverridden ? 'yes' : 'no (original revision)',
								],
							]}
						/>
					</CardContent>
				</Card>
			)}

			<div className='grid gap-6 lg:grid-cols-2'>
				<Card>
					<CardHeader>
						<CardTitle className='text-base'>Summary</CardTitle>
					</CardHeader>
					<CardContent>
						<KeyValueList
							items={[
								['Function', <Mono key='f'>{inv.function_id}</Mono>],
								['Revision', <Mono key='r'>{inv.revision_id}</Mono>],
								[
									'Alias',
									inv.alias
										? `${inv.alias} (generation ${inv.alias_generation ?? '?'})`
										: '— (pinned revision)',
								],
								['Trace id', <Mono key='t'>{inv.trace_id}</Mono>],
								['Accepted', formatDateTime(inv.accepted_at)],
								['Started', formatDateTime(inv.started_at)],
								['Finished', formatDateTime(inv.finished_at)],
								['HTTP status', inv.http_status ?? '—'],
								[
									'Queue deadline',
									formatDateTime(inv.deadlines.queue_deadline),
								],
								['Init deadline', formatDateTime(inv.deadlines.init_deadline)],
								[
									'Execution deadline',
									formatDateTime(inv.deadlines.execution_deadline),
								],
								[
									'Client deadline',
									formatDateTime(inv.deadlines.client_deadline),
								],
							]}
						/>
					</CardContent>
				</Card>
				<Card>
					<CardHeader>
						<CardTitle className='text-base'>Input and output</CardTitle>
						<CardDescription>
							Neither is shown by default. The API never returns the input body.
						</CardDescription>
					</CardHeader>
					<CardContent className='flex flex-col gap-4'>
						<KeyValueList
							items={[
								[
									'Input digest',
									<Mono key='d' testId='input-digest'>
										{inv.input_digest}
									</Mono>,
								],
								[
									'Input size',
									<span key='s' data-testid='input-size'>
										{formatBytes(inv.input_size_bytes)}
									</span>,
								],
							]}
						/>
						<div className='flex flex-col items-start gap-2'>
							<Button
								size='sm'
								variant='outline'
								onClick={() => setInputRevealRequested(true)}
								data-testid='reveal-input'
							>
								<Eye />
								Reveal input
							</Button>
							{inputRevealRequested && (
								<Alert data-testid='input-not-retained'>
									<ShieldQuestion className='size-4' />
									<AlertTitle>The input body is not retained</AlertTitle>
									<AlertDescription>
										The ledger stores only the SHA-256 digest and size of the
										input (synchronous invocations) or keeps the body for the
										dispatcher without exposing it (asynchronous), and the
										management API has no endpoint that returns it. Compare the
										digest with your own copy instead.
									</AlertDescription>
								</Alert>
							)}
						</div>
						<div className='flex flex-col items-start gap-2'>
							{outputText === null ? (
								<p className='text-sm text-muted-foreground'>
									No output recorded (failed, running, or past retention).
								</p>
							) : (
								<>
									<Button
										size='sm'
										variant='outline'
										onClick={() => setShowOutput(s => !s)}
										data-testid='toggle-output'
									>
										{showOutput ? <EyeOff /> : <Eye />}
										{showOutput
											? 'Hide output'
											: `Show output (${formatBytes(outputText.length)})`}
									</Button>
									{showOutput && (
										<JsonBlock value={inv.output} testId='invocation-output' />
									)}
								</>
							)}
						</div>
					</CardContent>
				</Card>
			</div>

			{inv.dispatch && (
				<Card data-testid='dispatch'>
					<CardHeader>
						<CardTitle className='text-base'>Asynchronous delivery</CardTitle>
						<CardDescription>
							Retries of an asynchronous invocation are further attempts of this
							same invocation.
						</CardDescription>
					</CardHeader>
					<CardContent>
						<KeyValueList
							items={[
								['State', inv.dispatch.state],
								['Counted attempts', inv.dispatch.attempts],
								['Deferrals (not counted)', inv.dispatch.deferrals],
								['Delivery generation', inv.dispatch.generation],
								['Next attempt', formatDateTime(inv.dispatch.next_attempt_at)],
								[
									'Last error',
									inv.dispatch.last_error
										? `${inv.dispatch.last_error.class} · ${inv.dispatch.last_error.error_type}: ${inv.dispatch.last_error.message}`
										: '—',
								],
								[
									'Dead letter',
									inv.dispatch.dead_letter_id ? (
										<Link
											key='dl'
											className='underline'
											href={routes.deadLetter(inv.dispatch.dead_letter_id)}
											data-testid='dead-letter-link'
										>
											<Mono>{inv.dispatch.dead_letter_id}</Mono>
										</Link>
									) : (
										'—'
									),
								],
							]}
						/>
					</CardContent>
				</Card>
			)}

			<Card>
				<CardHeader>
					<CardTitle className='text-base'>Attempts</CardTitle>
					<CardDescription>
						Attempt 1 is the initial run; later attempts are retries of this
						invocation. Timings are measured by the host, not reported by the
						guest.
					</CardDescription>
				</CardHeader>
				<CardContent>
					{(inv.attempts ?? []).length === 0 ? (
						<p
							className='text-sm text-muted-foreground'
							data-testid='no-attempts'
						>
							No attempt yet
							{isTerminal(inv.status)
								? ' (ended before an environment ran it)'
								: ''}
							.
						</p>
					) : (
						<div className='flex flex-col gap-3'>
							{(inv.attempts ?? []).map(a => (
								<AttemptRow key={a.id} inv={inv} attempt={a} />
							))}
						</div>
					)}
				</CardContent>
			</Card>

			<div id='logs'>
				<InvocationLogs inv={inv} attemptFilter={attemptFilter} />
			</div>
		</div>
	)
}

const TIMINGS: [keyof AttemptResponse['timings'], string][] = [
	['queue_wait_ms', 'queue wait'],
	['environment_boot_ms', 'environment boot'],
	['resume_ms', 'resume'],
	['runtime_init_ms', 'runtime init'],
	['readiness_ms', 'readiness'],
	['handler_ms', 'handler'],
	['response_ms', 'response'],
	['total_ms', 'total'],
]

function AttemptRow({
	inv,
	attempt,
}: { inv: InvocationResponse; attempt: AttemptResponse }) {
	const [showBoot, setShowBoot] = useState(false)
	const kind = attemptKind(attempt)
	return (
		<div
			id={`attempt-${attempt.id}`}
			className='rounded-md border p-3 target:ring-2 target:ring-ring'
			data-testid='attempt'
			data-attempt-kind={kind}
		>
			<div className='mb-2 flex flex-wrap items-center gap-2 text-sm'>
				<span className='font-semibold'>Attempt {attempt.number}</span>
				<Badge
					variant={kind === 'retry' ? 'outline' : 'secondary'}
					data-testid='attempt-kind'
				>
					{kind === 'retry' ? 'retry' : 'initial'}
				</Badge>
				<InvocationStatusBadge
					status={attempt.status}
					testId='attempt-status'
				/>
				<Badge variant='outline'>{attempt.start_kind} start</Badge>
				<Mono>{attempt.id}</Mono>
				<Link
					className='ml-auto underline'
					href={routes.invocationLogs(inv.id, attempt.id)}
					data-testid='attempt-logs-link'
				>
					Logs of this attempt
				</Link>
			</div>
			{attempt.error && (
				<p className='mb-2 text-sm text-destructive'>
					{attempt.error.class} · {attempt.error.error_type}:{' '}
					{attempt.error.message}
				</p>
			)}
			<Table>
				<TableHeader>
					<TableRow>
						{TIMINGS.map(([k, label]) => (
							<TableHead key={k}>{label}</TableHead>
						))}
					</TableRow>
				</TableHeader>
				<TableBody>
					<TableRow>
						{TIMINGS.map(([k]) => (
							<TableCell key={k}>{formatMs(attempt.timings[k])}</TableCell>
						))}
					</TableRow>
				</TableBody>
			</Table>
			<div className='mt-2 flex flex-wrap items-center gap-3 text-xs text-muted-foreground'>
				<span>
					environment <Mono>{attempt.environment_id}</Mono> · epoch{' '}
					{attempt.epoch}
				</span>
				<span>dispatched {formatDateTime(attempt.dispatched_at)}</span>
				<span>finished {formatDateTime(attempt.finished_at)}</span>
				<Button
					size='sm'
					variant='ghost'
					onClick={() => setShowBoot(s => !s)}
					data-testid='toggle-boot-evidence'
				>
					{showBoot ? 'Hide boot evidence' : 'Show boot evidence'}
				</Button>
			</div>
			{showBoot && (
				<JsonBlock value={attempt.boot_evidence} testId='boot-evidence' />
			)}
		</div>
	)
}
