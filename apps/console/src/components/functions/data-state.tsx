'use client'

import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import type { ApiError } from '@/lib/serverless-api/client'
import { useSession } from '@/lib/session'
import {
	AlertTriangle,
	Ban,
	Inbox,
	KeyRound,
	PlugZap,
	SearchX,
} from 'lucide-react'
import type { ReactNode } from 'react'
import type { SWRResponse } from 'swr'

export function LoadingState({
	rows = 3,
	label = 'Loading',
}: { rows?: number; label?: string }) {
	return (
		<div
			data-testid='state-loading'
			aria-busy='true'
			aria-label={label}
			className='flex flex-col gap-2'
		>
			{Array.from({ length: rows }).map((_, i) => (
				<Skeleton key={i} className='h-8 w-full' />
			))}
		</div>
	)
}

export function EmptyState({
	title,
	children,
}: { title: string; children?: ReactNode }) {
	return (
		<div
			data-testid='state-empty'
			className='flex flex-col items-center gap-2 rounded-md border border-dashed p-8 text-center text-sm text-muted-foreground'
		>
			<Inbox className='size-5' aria-hidden />
			<p className='font-medium text-foreground'>{title}</p>
			{children}
		</div>
	)
}

/**
 * Error, permission-denied, not-found and unauthorized states of one API call.
 * `resource` names what was requested ("function", "invocation", ...).
 */
export function ErrorState({
	error,
	resource,
	onRetry,
}: {
	error: ApiError
	resource: string
	onRetry?: () => void
}) {
	const { signOut } = useSession()
	if (error.status === 401) {
		return (
			<Alert variant='destructive' data-testid='state-unauthorized'>
				<KeyRound className='size-4' />
				<AlertTitle>Your token was not accepted</AlertTitle>
				<AlertDescription className='flex flex-col items-start gap-2'>
					<p>
						The gateway refused the credential ({error.code}). It may have been
						revoked, or the tenant id you entered does not match the token.
					</p>
					<Button size='sm' variant='outline' onClick={signOut}>
						Sign in again
					</Button>
				</AlertDescription>
			</Alert>
		)
	}
	if (error.status === 403) {
		return (
			<Alert variant='destructive' data-testid='state-forbidden'>
				<Ban className='size-4' />
				<AlertTitle>Permission denied</AlertTitle>
				<AlertDescription>
					<p>
						This token does not have the role needed to read this {resource}.
						Reading invocations, logs, usage and dead letters needs the{' '}
						<code>invoke</code> role; writes need <code>deploy</code>; redrive
						needs <code>invoke</code> and <code>redrive</code>.
					</p>
					<ErrorMeta error={error} />
				</AlertDescription>
			</Alert>
		)
	}
	if (error.isMissingRoute || error.code === 'async_unavailable') {
		return (
			<Alert data-testid='state-unavailable'>
				<PlugZap className='size-4' />
				<AlertTitle>Not available on this gateway</AlertTitle>
				<AlertDescription>
					<p>
						{error.code === 'async_unavailable'
							? 'Asynchronous invoke (queue, dead letters, redrive) is not configured on this gateway.'
							: `This gateway does not serve the API for this ${resource} yet.`}
					</p>
					<ErrorMeta error={error} />
				</AlertDescription>
			</Alert>
		)
	}
	if (error.status === 404) {
		return (
			<Alert data-testid='state-not-found'>
				<SearchX className='size-4' />
				<AlertTitle>Not found</AlertTitle>
				<AlertDescription>
					<p>
						This {resource} does not exist in your tenant. Resources of other
						tenants are always reported as not found.
					</p>
					<ErrorMeta error={error} />
				</AlertDescription>
			</Alert>
		)
	}
	return (
		<Alert variant='destructive' data-testid='state-error'>
			<AlertTriangle className='size-4' />
			<AlertTitle>Could not load the {resource}</AlertTitle>
			<AlertDescription className='flex flex-col items-start gap-2'>
				<p>{error.message}</p>
				<ErrorMeta error={error} />
				{onRetry && (
					<Button size='sm' variant='outline' onClick={onRetry}>
						Retry
					</Button>
				)}
			</AlertDescription>
		</Alert>
	)
}

export function ErrorMeta({ error }: { error: ApiError }) {
	return (
		<p className='mt-1 font-mono text-xs opacity-80'>
			{error.status > 0 ? `HTTP ${error.status} · ` : ''}
			{error.code}
			{error.reason ? ` · reason=${error.reason}` : ''}
			{error.errorType ? ` · ${error.errorType}` : ''}
			{error.requestId ? ` · request ${error.requestId}` : ''}
		</p>
	)
}

/** Renders loading / error / empty / data for one SWR call. */
export function DataState<T>({
	query,
	resource,
	isEmpty,
	empty,
	children,
	rows,
}: {
	query: SWRResponse<T, ApiError>
	resource: string
	isEmpty?: (data: T) => boolean
	empty?: ReactNode
	children: (data: T) => ReactNode
	rows?: number
}) {
	if (query.error) {
		return (
			<ErrorState
				error={query.error}
				resource={resource}
				onRetry={() => query.mutate()}
			/>
		)
	}
	if (query.data === undefined) return <LoadingState rows={rows} />
	if (isEmpty?.(query.data))
		return <>{empty ?? <EmptyState title={`No ${resource}s yet`} />}</>
	return <>{children(query.data)}</>
}
