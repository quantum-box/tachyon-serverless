import { cn } from '@/lib/utils'

type SkeletonProps = React.HTMLAttributes<HTMLDivElement> & {
	'data-testid'?: string
}

function Skeleton({
	className,
	'data-testid': dataTestId = 'skeleton',
	...props
}: SkeletonProps) {
	return (
		<div
			className={cn('animate-pulse rounded-md bg-primary/10', className)}
			data-testid={dataTestId}
			{...props}
		/>
	)
}

export { Skeleton }
