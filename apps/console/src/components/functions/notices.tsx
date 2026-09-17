import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import {
	OUTCOME_UNKNOWN_EXPLANATION,
	outcomeUnknownFollowUp,
} from '@/lib/invocation-kind'
import { CircleHelp, Receipt } from 'lucide-react'

export function OutcomeUnknownNotice({ mode }: { mode?: string }) {
	return (
		<Alert className='border-warning' data-testid='outcome-unknown-notice'>
			<CircleHelp className='size-4' />
			<AlertTitle>Outcome unknown</AlertTitle>
			<AlertDescription className='flex flex-col gap-1'>
				<p>{OUTCOME_UNKNOWN_EXPLANATION}</p>
				<p>{outcomeUnknownFollowUp(mode)}</p>
			</AlertDescription>
		</Alert>
	)
}

/** Shown on every page with money-shaped numbers. */
export function ProvisionalBanner({ notice }: { notice?: string }) {
	return (
		<Alert
			variant='destructive'
			data-testid='provisional-banner'
			className='border-2'
		>
			<Receipt className='size-4' />
			<AlertTitle>Provisional estimate — not an invoice</AlertTitle>
			<AlertDescription className='flex flex-col gap-1'>
				<p>
					These amounts are a prototype estimate from a provisional price table.
					Nothing is charged and billing is disabled. Do not use them as a bill
					or a quote.
				</p>
				{notice && <p className='font-mono text-xs'>API notice: {notice}</p>}
			</AlertDescription>
		</Alert>
	)
}
