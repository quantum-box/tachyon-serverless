import * as React from 'react'

import { cn } from '@/lib/utils'

export interface TextareaProps
	extends React.TextareaHTMLAttributes<HTMLTextAreaElement> {
	/** Error message — sets aria-invalid and aria-describedby */
	error?: string
}

const Textarea = React.forwardRef<HTMLTextAreaElement, TextareaProps>(
	({ className, error, id, ...props }, ref) => {
		const errorId = error && id ? `${id}-error` : undefined
		return (
			<>
				<textarea
					id={id}
					className={cn(
						'flex min-h-[80px] w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50',
						error && 'border-destructive focus-visible:ring-destructive',
						className,
					)}
					ref={ref}
					aria-invalid={error ? true : undefined}
					aria-describedby={errorId}
					{...props}
				/>
				{error && errorId && (
					<p id={errorId} className='text-sm text-destructive mt-1' role='alert'>
						{error}
					</p>
				)}
			</>
		)
	},
)
Textarea.displayName = 'Textarea'

export { Textarea }
