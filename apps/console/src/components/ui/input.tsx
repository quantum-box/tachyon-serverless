import * as React from 'react'

import { cn } from '@/lib/utils'

export interface InputProps
	extends React.InputHTMLAttributes<HTMLInputElement> {
	/** Error message — sets aria-invalid and aria-describedby */
	error?: string
}

const Input = React.forwardRef<HTMLInputElement, InputProps>(
	({ className, type, error, id, ...props }, ref) => {
		const errorId = error && id ? `${id}-error` : undefined
		return (
			<>
				<input
					type={type}
					id={id}
					className={cn(
						'flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50',
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
Input.displayName = 'Input'

export { Input }
