'use client'

import { LoadingState } from '@/components/functions/data-state'
import { Button } from '@/components/ui/button'
import { routes } from '@/lib/navigation'
import { useSession } from '@/lib/session'
import { cn } from '@/lib/utils'
import {
	Activity,
	FunctionSquare,
	Gauge,
	LogOut,
	Receipt,
	Wallet,
} from 'lucide-react'
import Link from 'next/link'
import { usePathname, useRouter } from 'next/navigation'
import { type ReactNode, useEffect } from 'react'
import { useSWRConfig } from 'swr'

const NAV = [
	{
		href: routes.functions,
		label: 'Functions',
		icon: FunctionSquare,
		match: ['/functions', '/invocations', '/dead-letters'],
	},
	{ href: routes.usage, label: 'Usage', icon: Receipt, match: ['/usage'] },
	{ href: routes.budget, label: 'Budget', icon: Wallet, match: ['/budget'] },
	{
		href: routes.capacity,
		label: 'Capacity',
		icon: Gauge,
		match: ['/capacity'],
	},
]

export function AppShell({ children }: { children: ReactNode }) {
	const { credentials, signOut } = useSession()
	const router = useRouter()
	const pathname = usePathname()
	const { mutate } = useSWRConfig()

	useEffect(() => {
		if (credentials === null) {
			const here = `${window.location.pathname.replace(/^\/console/, '')}${window.location.search}${window.location.hash}`
			router.replace(`/?next=${encodeURIComponent(here)}`)
		}
	}, [credentials, router])

	if (!credentials) {
		return (
			<main className='p-6'>
				<LoadingState rows={2} label='Checking session' />
			</main>
		)
	}

	return (
		<div className='flex min-h-screen flex-col'>
			<header className='border-b'>
				<div className='mx-auto flex max-w-7xl flex-wrap items-center gap-4 px-4 py-2'>
					<Link
						href={routes.functions}
						className='flex items-center gap-2 font-semibold'
					>
						<Activity className='size-4' aria-hidden />
						Tachyon Serverless
						<span className='rounded border px-1.5 text-xs font-normal text-muted-foreground'>
							prototype
						</span>
					</Link>
					<nav className='flex flex-wrap gap-1' aria-label='Console'>
						{NAV.map(item => {
							const active = item.match.some(m => pathname?.startsWith(m))
							return (
								<Link
									key={item.href}
									href={item.href}
									aria-current={active ? 'page' : undefined}
									className={cn(
										'flex items-center gap-1.5 rounded-md px-2.5 py-1.5 text-sm hover:bg-accent',
										active && 'bg-accent font-medium',
									)}
								>
									<item.icon className='size-4' aria-hidden />
									{item.label}
								</Link>
							)
						})}
					</nav>
					<div className='ml-auto flex items-center gap-3 text-xs text-muted-foreground'>
						<span data-testid='session-tenant'>
							{credentials.tenantId
								? `tenant ${credentials.tenantId}`
								: 'tenant of the token'}
						</span>
						<Button
							size='sm'
							variant='ghost'
							onClick={() => {
								// Drop every cached response of this tenant before the session goes.
								mutate(() => true, undefined, { revalidate: false })
								signOut()
							}}
							data-testid='sign-out'
						>
							<LogOut />
							Sign out
						</Button>
					</div>
				</div>
			</header>
			<main className='mx-auto flex w-full max-w-7xl flex-1 flex-col gap-6 px-4 py-6'>
				{children}
			</main>
		</div>
	)
}
