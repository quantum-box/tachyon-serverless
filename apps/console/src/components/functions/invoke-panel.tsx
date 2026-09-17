'use client'

import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from '@/components/ui/card'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { routes } from '@/lib/navigation'
import { notifyFailure, notifySuccess } from '@/lib/notify'
import {
	type ApiError,
	apiPath,
	apiRequest,
	toApiError,
} from '@/lib/serverless-api/client'
import { useApi } from '@/lib/serverless-api/hooks'
import type {
	AliasResponse,
	FunctionResponse,
	InvokeAsyncResponse,
	ListResponse,
	RevisionResponse,
} from '@/lib/serverless-api/types'
import { useCredentials } from '@/lib/session'
import { Loader2, Play } from 'lucide-react'
import Link from 'next/link'
import { type FormEvent, useState } from 'react'
import { ErrorMeta } from './data-state'
import { JsonBlock, Mono } from './kv'
import { OutcomeUnknownNotice } from './notices'

type Target =
	| { kind: 'alias'; value: string }
	| { kind: 'revision'; value: string }

type Result =
	| {
			kind: 'sync-ok'
			invocationId: string | null
			output: unknown
			ms: number
	  }
	| { kind: 'async-accepted'; body: InvokeAsyncResponse }
	| { kind: 'error'; error: ApiError; mode: 'sync' | 'async' }

const selectClass =
	'flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'

export function InvokePanel({ fn }: { fn: FunctionResponse }) {
	const credentials = useCredentials()
	const aliases = useApi<ListResponse<AliasResponse>>(
		apiPath`/v1/functions/${fn.id}/aliases`,
	)
	const revisions = useApi<ListResponse<RevisionResponse>>(
		apiPath`/v1/functions/${fn.id}/revisions`,
	)
	const [target, setTarget] = useState<string>('alias:prod')
	const [mode, setMode] = useState<'sync' | 'async'>('sync')
	// The input exists only in this component's state: it is not stored,
	// not put in the URL and gone when the page is left.
	const [input, setInput] = useState('{}')
	const [inputError, setInputError] = useState<string | null>(null)
	const [pending, setPending] = useState(false)
	const [result, setResult] = useState<Result | null>(null)

	const aliasOptions = aliases.data?.items ?? []
	const readyRevisions = (revisions.data?.items ?? []).filter(
		r => r.status === 'ready',
	)

	function parseTarget(): Target {
		const [kind, ...rest] = target.split(':')
		return {
			kind: kind === 'revision' ? 'revision' : 'alias',
			value: rest.join(':'),
		}
	}

	async function submit(e: FormEvent) {
		e.preventDefault()
		let body: unknown
		try {
			body = JSON.parse(input)
		} catch (err) {
			setInputError(
				`Input is not valid JSON: ${err instanceof Error ? err.message : 'parse error'}`,
			)
			return
		}
		setInputError(null)
		setPending(true)
		setResult(null)
		const t = parseTarget()
		const query =
			t.kind === 'alias' ? { alias: t.value } : { revision_id: t.value }
		const started = performance.now()
		try {
			if (mode === 'sync') {
				const res = await apiRequest<unknown>(
					credentials,
					apiPath`/v1/functions/${fn.id}/invoke`,
					{
						method: 'POST',
						query,
						body,
					},
				)
				const invocationId = res.headers.get('x-tachyon-invocation-id')
				setResult({
					kind: 'sync-ok',
					invocationId,
					output: res.data,
					ms: Math.round(performance.now() - started),
				})
				notifySuccess(
					'Invocation succeeded',
					invocationId ? `New invocation ${invocationId}` : undefined,
				)
			} else {
				const res = await apiRequest<InvokeAsyncResponse>(
					credentials,
					apiPath`/v1/functions/${fn.id}/invokeAsync`,
					{ method: 'POST', query, body },
				)
				setResult({ kind: 'async-accepted', body: res.data })
				notifySuccess(
					'Asynchronous invocation accepted',
					`New invocation ${res.data.invocation_id}`,
				)
			}
		} catch (err) {
			const error = toApiError(err)
			setResult({ kind: 'error', error, mode })
			notifyFailure(
				mode === 'sync' ? 'Invocation failed' : 'Asynchronous invoke refused',
				error,
			)
		} finally {
			setPending(false)
		}
	}

	return (
		<div className='grid gap-6 lg:grid-cols-2'>
			<Card>
				<CardHeader>
					<CardTitle className='text-base'>Test invoke</CardTitle>
					<CardDescription>
						Every run creates a <strong>new invocation</strong> (it is not a
						retry of an earlier one). The input below is sent once and is not
						kept by the console; the API stores only its SHA-256 digest and
						size.
					</CardDescription>
				</CardHeader>
				<CardContent>
					<form
						className='flex flex-col gap-4'
						onSubmit={submit}
						data-testid='invoke-form'
					>
						<div className='grid gap-4 sm:grid-cols-2'>
							<div className='flex flex-col gap-2'>
								<Label htmlFor='invoke-target'>Target</Label>
								<select
									id='invoke-target'
									data-testid='invoke-target'
									className={selectClass}
									value={target}
									onChange={e => setTarget(e.target.value)}
								>
									{aliasOptions.length === 0 && (
										<option value='alias:prod'>alias prod</option>
									)}
									{aliasOptions.map(a => (
										<option key={a.name} value={`alias:${a.name}`}>
											alias {a.name}
										</option>
									))}
									{readyRevisions.map(r => (
										<option key={r.id} value={`revision:${r.id}`}>
											revision #{r.number} (pinned)
										</option>
									))}
								</select>
							</div>
							<fieldset className='flex flex-col gap-2'>
								<legend className='mb-2 text-sm font-medium leading-none'>
									Mode
								</legend>
								<div className='flex h-10 items-center gap-4 text-sm'>
									<label className='flex items-center gap-2'>
										<input
											type='radio'
											name='mode'
											value='sync'
											data-testid='invoke-mode-sync'
											checked={mode === 'sync'}
											onChange={() => setMode('sync')}
										/>
										Synchronous
									</label>
									<label className='flex items-center gap-2'>
										<input
											type='radio'
											name='mode'
											value='async'
											data-testid='invoke-mode-async'
											checked={mode === 'async'}
											onChange={() => setMode('async')}
										/>
										Asynchronous
									</label>
								</div>
							</fieldset>
						</div>
						<div className='flex flex-col gap-2'>
							<Label htmlFor='invoke-input'>Input (JSON)</Label>
							<Textarea
								id='invoke-input'
								data-testid='invoke-input'
								className='min-h-40 font-mono text-xs'
								spellCheck={false}
								autoComplete='off'
								value={input}
								error={inputError ?? undefined}
								onChange={e => setInput(e.target.value)}
							/>
						</div>
						<Button
							type='submit'
							disabled={pending}
							data-testid='invoke-submit'
						>
							{pending ? <Loader2 className='animate-spin' /> : <Play />}
							Invoke
						</Button>
					</form>
				</CardContent>
			</Card>
			<Card>
				<CardHeader>
					<CardTitle className='text-base'>Result</CardTitle>
					<CardDescription>
						Only the latest run is shown; nothing is kept after you leave the
						page.
					</CardDescription>
				</CardHeader>
				<CardContent data-testid='invoke-result'>
					{!result && !pending && (
						<p className='text-sm text-muted-foreground'>No invocation yet.</p>
					)}
					{pending && (
						<p
							className='flex items-center gap-2 text-sm text-muted-foreground'
							data-testid='invoke-pending'
						>
							<Loader2 className='size-4 animate-spin' /> Waiting for the
							gateway…
						</p>
					)}
					{result?.kind === 'sync-ok' && (
						<div className='flex flex-col gap-3' data-testid='invoke-result-ok'>
							<p className='text-sm'>
								Succeeded in {result.ms} ms (round trip).{' '}
								{result.invocationId && (
									<Link
										className='underline'
										href={routes.invocation(result.invocationId)}
										data-testid='invoke-result-link'
									>
										Open invocation <Mono>{result.invocationId}</Mono>
									</Link>
								)}
							</p>
							<JsonBlock value={result.output} testId='invoke-output' />
						</div>
					)}
					{result?.kind === 'async-accepted' && (
						<div
							className='flex flex-col gap-2 text-sm'
							data-testid='invoke-result-async'
						>
							<p>
								Accepted as a new asynchronous invocation (status{' '}
								<strong>{result.body.status}</strong>
								{result.body.replayed ? ', replayed' : ''}). It runs in the
								background with retries; follow it on its page.
							</p>
							<Link
								className='underline'
								href={routes.invocation(result.body.invocation_id)}
								data-testid='invoke-result-link'
							>
								Open invocation <Mono>{result.body.invocation_id}</Mono>
							</Link>
							<p className='text-xs text-muted-foreground'>
								input digest <Mono>{result.body.input_digest}</Mono> ·{' '}
								{result.body.input_size_bytes} B
							</p>
						</div>
					)}
					{result?.kind === 'error' && (
						<div
							className='flex flex-col gap-3'
							data-testid='invoke-result-error'
						>
							{result.error.code === 'outcome_unknown' && (
								<OutcomeUnknownNotice mode={result.mode} />
							)}
							<Alert variant='destructive'>
								<AlertTitle data-testid='invoke-error-code'>
									{result.error.code}
								</AlertTitle>
								<AlertDescription>
									<p>{result.error.message}</p>
									<ErrorMeta error={result.error} />
									{result.error.invocationId && (
										<Link
											className='mt-2 inline-block underline'
											href={routes.invocation(result.error.invocationId)}
											data-testid='invoke-result-link'
										>
											Open invocation <Mono>{result.error.invocationId}</Mono>{' '}
											(attempts, logs)
										</Link>
									)}
								</AlertDescription>
							</Alert>
						</div>
					)}
				</CardContent>
			</Card>
		</div>
	)
}
