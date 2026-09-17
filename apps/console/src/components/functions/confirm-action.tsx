'use client'

import {
	AlertDialog,
	AlertDialogCancel,
	AlertDialogContent,
	AlertDialogDescription,
	AlertDialogFooter,
	AlertDialogHeader,
	AlertDialogTitle,
	AlertDialogTrigger,
} from '@/components/ui/alert-dialog'
import { Button, type ButtonProps } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { Loader2 } from 'lucide-react'
import { type ReactNode, useState } from 'react'

/**
 * A destructive or state-changing action behind a confirmation dialog.
 * `onConfirm` runs only after the viewer confirms; the dialog stays open
 * (with the action disabled) until it settles, and the caller reports the
 * result with a toast.
 */
export function ConfirmAction({
	trigger,
	triggerVariant = 'outline',
	triggerDisabled,
	title,
	description,
	confirmLabel,
	reasonLabel,
	testId,
	onConfirm,
}: {
	trigger: ReactNode
	triggerVariant?: ButtonProps['variant']
	triggerDisabled?: boolean
	title: string
	description: ReactNode
	confirmLabel: string
	/** When set, the dialog asks for a free-text reason passed to `onConfirm`. */
	reasonLabel?: string
	testId: string
	onConfirm: (reason: string) => Promise<void>
}) {
	const [open, setOpen] = useState(false)
	const [pending, setPending] = useState(false)
	const [reason, setReason] = useState('')

	async function confirm() {
		setPending(true)
		try {
			await onConfirm(reason.trim())
		} finally {
			setPending(false)
			setOpen(false)
			setReason('')
		}
	}

	return (
		<AlertDialog open={open} onOpenChange={o => !pending && setOpen(o)}>
			<AlertDialogTrigger asChild>
				<Button
					size='sm'
					variant={triggerVariant}
					disabled={triggerDisabled}
					data-testid={`${testId}-trigger`}
				>
					{trigger}
				</Button>
			</AlertDialogTrigger>
			<AlertDialogContent data-testid={`${testId}-dialog`}>
				<AlertDialogHeader>
					<AlertDialogTitle>{title}</AlertDialogTitle>
					<AlertDialogDescription asChild>
						<div className='flex flex-col gap-2'>{description}</div>
					</AlertDialogDescription>
				</AlertDialogHeader>
				{reasonLabel && (
					<div className='flex flex-col gap-2'>
						<Label htmlFor={`${testId}-reason`}>{reasonLabel}</Label>
						<Textarea
							id={`${testId}-reason`}
							data-testid={`${testId}-reason`}
							value={reason}
							maxLength={1024}
							onChange={e => setReason(e.target.value)}
						/>
					</div>
				)}
				<AlertDialogFooter>
					<AlertDialogCancel disabled={pending}>Cancel</AlertDialogCancel>
					<Button
						variant='destructive'
						onClick={confirm}
						disabled={pending}
						data-testid={`${testId}-confirm`}
					>
						{pending && <Loader2 className='animate-spin' />}
						{confirmLabel}
					</Button>
				</AlertDialogFooter>
			</AlertDialogContent>
		</AlertDialog>
	)
}
