import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'
import { Loader2 } from 'lucide-react'

// Colors come only from the native-ui preset tokens (success / warning /
// destructive / secondary); no console-specific palette.
const INVOCATION: Record<string, string> = {
	succeeded: 'border-transparent bg-success text-primary-foreground',
	failed: 'border-transparent bg-destructive text-destructive-foreground',
	outcome_unknown: 'border-transparent bg-warning text-foreground',
	cancelled: 'border-transparent bg-secondary text-secondary-foreground',
}

export function InvocationStatusBadge({
	status,
	testId = 'invocation-status',
}: { status: string; testId?: string }) {
	const settled = status in INVOCATION
	return (
		<Badge
			variant='outline'
			data-testid={testId}
			data-status={status}
			className={cn('gap-1 whitespace-nowrap', INVOCATION[status])}
		>
			{!settled && <Loader2 className='size-3 animate-spin' aria-hidden />}
			{status === 'outcome_unknown' ? 'outcome unknown' : status}
		</Badge>
	)
}

const REVISION: Record<string, string> = {
	ready: 'border-transparent bg-success text-primary-foreground',
	failed: 'border-transparent bg-destructive text-destructive-foreground',
}

export function RevisionStatusBadge({ status }: { status: string }) {
	const settled = status in REVISION
	return (
		<Badge
			variant='outline'
			data-testid='revision-status'
			data-status={status}
			className={cn('gap-1 whitespace-nowrap', REVISION[status])}
		>
			{!settled && <Loader2 className='size-3 animate-spin' aria-hidden />}
			{status}
		</Badge>
	)
}

const DEAD_LETTER: Record<string, string> = {
	open: 'border-transparent bg-destructive text-destructive-foreground',
	redriven: 'border-transparent bg-secondary text-secondary-foreground',
}

export function DeadLetterStatusBadge({ status }: { status: string }) {
	return (
		<Badge
			variant='outline'
			data-testid='dead-letter-status'
			data-status={status}
			className={cn('whitespace-nowrap', DEAD_LETTER[status])}
		>
			{status}
		</Badge>
	)
}
