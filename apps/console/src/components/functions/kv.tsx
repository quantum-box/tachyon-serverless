import type { ReactNode } from 'react'

export function KeyValueList({ items }: { items: [ReactNode, ReactNode][] }) {
	return (
		<dl className='grid grid-cols-1 gap-x-6 gap-y-2 text-sm sm:grid-cols-[max-content_1fr]'>
			{items.map(([k, v], i) => (
				<div key={i} className='contents'>
					<dt className='text-muted-foreground'>{k}</dt>
					<dd className='min-w-0 break-all'>{v}</dd>
				</div>
			))}
		</dl>
	)
}

export function Mono({
	children,
	testId,
}: { children: ReactNode; testId?: string }) {
	return (
		<span className='font-mono text-xs' data-testid={testId}>
			{children}
		</span>
	)
}

export function JsonBlock({
	value,
	testId,
}: { value: unknown; testId?: string }) {
	return (
		<pre
			data-testid={testId}
			className='max-h-96 overflow-auto rounded-md border bg-muted p-3 font-mono text-xs'
		>
			{JSON.stringify(value, null, 2)}
		</pre>
	)
}

export function PageHeader({
	title,
	description,
	actions,
	breadcrumbs,
}: {
	title: ReactNode
	description?: ReactNode
	actions?: ReactNode
	breadcrumbs?: ReactNode
}) {
	return (
		<div className='flex flex-col gap-2'>
			{breadcrumbs && (
				<nav className='text-xs text-muted-foreground'>{breadcrumbs}</nav>
			)}
			<div className='flex flex-wrap items-start justify-between gap-4'>
				<div className='min-w-0'>
					<h1 className='break-all text-xl font-semibold'>{title}</h1>
					{description && (
						<div className='text-sm text-muted-foreground'>{description}</div>
					)}
				</div>
				{actions && <div className='flex flex-wrap gap-2'>{actions}</div>}
			</div>
		</div>
	)
}
