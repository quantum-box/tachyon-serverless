import Link from 'next/link'

export default function NotFound() {
	return (
		<main className='flex min-h-screen flex-col items-center justify-center gap-2 p-4 text-center'>
			<h1 className='text-xl font-semibold'>Page not found</h1>
			<p className='text-sm text-muted-foreground'>
				This console page does not exist.
			</p>
			<Link className='underline' href='/functions/'>
				Back to functions
			</Link>
		</main>
	)
}
