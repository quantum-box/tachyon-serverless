'use client'

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
import { formatDateTime, shortId } from '@/lib/format'
import { routes } from '@/lib/navigation'
import { notifyFailure, notifySuccess } from '@/lib/notify'
import { apiPath, apiRequest } from '@/lib/serverless-api/client'
import { useApi } from '@/lib/serverless-api/hooks'
import {
	type AliasResponse,
	type FunctionResponse,
	type ListResponse,
	type RevisionResponse,
	isRevisionSettling,
} from '@/lib/serverless-api/types'
import { useCredentials } from '@/lib/session'
import { Eye, EyeOff, Undo2 } from 'lucide-react'
import Link from 'next/link'
import { useState } from 'react'
import { ConfirmAction } from './confirm-action'
import { DataState, EmptyState } from './data-state'
import { KeyValueList, Mono } from './kv'
import { RevisionStatusBadge } from './status-badge'

export function RevisionsPanel({ fn }: { fn: FunctionResponse }) {
	const revisions = useApi<ListResponse<RevisionResponse>>(
		apiPath`/v1/functions/${fn.id}/revisions`,
		undefined,
		{
			// Poll while a deploy is still being prepared / validated.
			refreshInterval: data =>
				data?.items.some(r => isRevisionSettling(r.status)) ? 1500 : 0,
		},
	)
	const aliases = useApi<ListResponse<AliasResponse>>(
		apiPath`/v1/functions/${fn.id}/aliases`,
	)
	const byRevision = new Map<string, AliasResponse[]>()
	for (const a of aliases.data?.items ?? []) {
		byRevision.set(a.revision_id, [...(byRevision.get(a.revision_id) ?? []), a])
	}
	const numberOf = (id: string | null | undefined) =>
		revisions.data?.items.find(r => r.id === id)?.number

	return (
		<div className='flex flex-col gap-6'>
			<Card>
				<CardHeader>
					<CardTitle className='text-base'>Aliases</CardTitle>
					<CardDescription>
						Invokes resolve an alias to a revision when they are accepted.
						Rolling back points the alias at its previous revision; invocations
						already accepted keep their revision.
					</CardDescription>
				</CardHeader>
				<CardContent>
					<DataState
						query={aliases}
						resource='alias'
						isEmpty={d => d.items.length === 0}
						empty={
							<EmptyState title='No aliases yet'>
								A revision published to prod creates the prod alias.
							</EmptyState>
						}
					>
						{data => (
							<Table data-testid='aliases-table'>
								<TableHeader>
									<TableRow>
										<TableHead>Alias</TableHead>
										<TableHead>Revision</TableHead>
										<TableHead>Previous</TableHead>
										<TableHead>Generation</TableHead>
										<TableHead>Updated</TableHead>
										<TableHead />
									</TableRow>
								</TableHeader>
								<TableBody>
									{data.items.map(a => (
										<TableRow
											key={a.name}
											data-testid='alias-row'
											data-alias={a.name}
										>
											<TableCell className='font-medium'>{a.name}</TableCell>
											<TableCell>
												<Link
													className='hover:underline'
													href={routes.revision(fn.id, a.revision_id)}
												>
													<span data-testid='alias-revision'>
														#{numberOf(a.revision_id) ?? '?'}
													</span>{' '}
													<Mono>{shortId(a.revision_id)}</Mono>
												</Link>
											</TableCell>
											<TableCell>
												{a.previous_revision_id ? (
													<>
														#{numberOf(a.previous_revision_id) ?? '?'}{' '}
														<Mono>{shortId(a.previous_revision_id)}</Mono>
													</>
												) : (
													'—'
												)}
											</TableCell>
											<TableCell>{a.generation}</TableCell>
											<TableCell>{formatDateTime(a.updated_at)}</TableCell>
											<TableCell className='text-right'>
												<RollbackButton
													fn={fn}
													alias={a}
													fromNumber={numberOf(a.revision_id)}
													toNumber={numberOf(a.previous_revision_id)}
													onDone={() => {
														aliases.mutate()
														revisions.mutate()
													}}
												/>
											</TableCell>
										</TableRow>
									))}
								</TableBody>
							</Table>
						)}
					</DataState>
				</CardContent>
			</Card>

			<Card>
				<CardHeader>
					<CardTitle className='text-base'>Revisions</CardTitle>
					<CardDescription>
						Deploy state of every revision: pending → preparing → validating →
						ready, or failed with a reason. Secret values never reach the API;
						only binding names are shown.
					</CardDescription>
				</CardHeader>
				<CardContent>
					<DataState
						query={revisions}
						resource='revision'
						isEmpty={d => d.items.length === 0}
						empty={
							<EmptyState title='No revisions yet'>
								Deploy one with <code>tsls functions deploy</code>.
							</EmptyState>
						}
					>
						{data => (
							<div className='flex flex-col gap-3' data-testid='revisions-list'>
								{[...data.items]
									.sort((a, b) => b.number - a.number)
									.map(r => (
										<RevisionCard
											key={r.id}
											revision={r}
											aliases={byRevision.get(r.id) ?? []}
										/>
									))}
							</div>
						)}
					</DataState>
				</CardContent>
			</Card>
		</div>
	)
}

function RollbackButton({
	fn,
	alias,
	fromNumber,
	toNumber,
	onDone,
}: {
	fn: FunctionResponse
	alias: AliasResponse
	fromNumber?: number
	toNumber?: number
	onDone: () => void
}) {
	const credentials = useCredentials()
	const previous = alias.previous_revision_id
	return (
		<ConfirmAction
			testId='rollback'
			trigger={
				<>
					<Undo2 />
					Roll back
				</>
			}
			triggerDisabled={!previous}
			title={`Roll back alias ${alias.name}?`}
			description={
				<>
					<p>
						Alias <strong>{alias.name}</strong> will point at revision #
						{toNumber ?? '?'} (<code>{previous}</code>) instead of #
						{fromNumber ?? '?'} (<code>{alias.revision_id}</code>).
					</p>
					<p>
						New invocations use the previous revision at once. Invocations
						already accepted keep their revision. The update is refused if
						someone changed the alias in the meantime (generation{' '}
						{alias.generation}).
					</p>
				</>
			}
			confirmLabel='Roll back'
			onConfirm={async () => {
				if (!previous) return
				try {
					const res = await apiRequest<AliasResponse>(
						credentials,
						apiPath`/v1/functions/${fn.id}/aliases/${alias.name}`,
						{
							method: 'PUT',
							body: {
								revision_id: previous,
								expected_generation: alias.generation,
							},
						},
					)
					notifySuccess(
						`Alias ${alias.name} rolled back`,
						`Now on revision #${toNumber ?? '?'} (generation ${res.data.generation}).`,
					)
				} catch (e) {
					notifyFailure(`Rollback of ${alias.name} failed`, e)
				} finally {
					onDone()
				}
			}}
		/>
	)
}

type Spec = {
	artifact?: {
		kind?: string
		digest?: string
		size_bytes?: number
		reference?: string
	}
	runtime?: { architecture?: string; protocol?: string }
	resources?: Record<string, number>
	execution?: Record<string, number>
	egress?: string
	env_vars?: [string, string][]
	secrets?: { env_name: string; binding_ref: string }[]
	description?: string
}

function RevisionCard({
	revision,
	aliases,
}: { revision: RevisionResponse; aliases: AliasResponse[] }) {
	const spec = revision.spec as unknown as Spec
	const [showEnv, setShowEnv] = useState(false)
	return (
		<div
			id={`revision-${revision.id}`}
			data-testid='revision-card'
			data-revision-id={revision.id}
			className='rounded-md border p-4 target:ring-2 target:ring-ring'
		>
			<div className='mb-3 flex flex-wrap items-center gap-2'>
				<span className='font-semibold'>#{revision.number}</span>
				<RevisionStatusBadge status={revision.status} />
				{aliases.map(a => (
					<span
						key={a.name}
						className='rounded border px-1.5 text-xs'
						data-testid='revision-alias'
					>
						{a.name}
					</span>
				))}
				<Mono>{revision.id}</Mono>
				{spec.description && (
					<span className='text-sm text-muted-foreground'>
						{spec.description}
					</span>
				)}
			</div>
			{revision.status === 'failed' && revision.failure_reason && (
				<p
					className='mb-3 text-sm text-destructive'
					data-testid='revision-failure'
				>
					{revision.failure_reason}
				</p>
			)}
			<KeyValueList
				items={[
					[
						'Artifact',
						<Mono key='a'>
							{spec.artifact?.digest ?? spec.artifact?.reference ?? '—'}
						</Mono>,
					],
					['Architecture', spec.runtime?.architecture ?? '—'],
					[
						'Resources',
						spec.resources
							? Object.entries(spec.resources)
									.map(([k, v]) => `${k}=${v}`)
									.join(' · ')
							: '—',
					],
					[
						'Execution',
						spec.execution
							? Object.entries(spec.execution)
									.map(([k, v]) => `${k}=${v}`)
									.join(' · ')
							: '—',
					],
					['Egress', spec.egress ?? 'none'],
					[
						'Secrets',
						spec.secrets?.length ? (
							<span key='s' data-testid='revision-secrets'>
								{spec.secrets
									.map(s => `${s.env_name} ← binding ${s.binding_ref}`)
									.join(', ')}{' '}
								<span className='text-xs text-muted-foreground'>
									(values are never exposed by the API)
								</span>
							</span>
						) : (
							'none'
						),
					],
					[
						'Environment',
						spec.env_vars?.length ? (
							<span key='e' className='flex flex-wrap items-center gap-2'>
								<span data-testid='revision-env'>
									{spec.env_vars
										.map(([k, v]) => (showEnv ? `${k}=${v}` : `${k}=••••`))
										.join(', ')}
								</span>
								<Button
									size='sm'
									variant='ghost'
									onClick={() => setShowEnv(s => !s)}
								>
									{showEnv ? <EyeOff /> : <Eye />}
									{showEnv ? 'Hide values' : 'Show values'}
								</Button>
							</span>
						) : (
							'none'
						),
					],
					['Spec digest', <Mono key='d'>{revision.spec_digest}</Mono>],
					['Created', formatDateTime(revision.created_at)],
				]}
			/>
		</div>
	)
}
