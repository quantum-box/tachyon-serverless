'use client'

import { toast } from '@/components/ui/use-toast'
import type { ReactNode } from 'react'
import { toApiError } from './serverless-api/client'

/** Result notification of a state-changing action (rollback, cancel, redrive, invoke). */
export function notifySuccess(title: string, description?: ReactNode) {
	toast({ title, description, 'data-testid': 'toast-success' } as Parameters<
		typeof toast
	>[0])
}

export function notifyFailure(title: string, error: unknown) {
	const e = toApiError(error)
	const detail = [
		e.status > 0 ? `HTTP ${e.status}` : null,
		e.code,
		e.reason ? `reason=${e.reason}` : null,
	]
		.filter(Boolean)
		.join(' · ')
	toast({
		title,
		description: `${e.message} (${detail})`,
		variant: 'destructive',
		'data-testid': 'toast-failure',
	} as Parameters<typeof toast>[0])
}
