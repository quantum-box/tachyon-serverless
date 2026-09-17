'use client'

import { ErrorMeta } from '@/components/functions/data-state'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { safeNextPath } from '@/lib/navigation'
import {
	type ApiError,
	apiRequest,
	toApiError,
} from '@/lib/serverless-api/client'
import { useSession } from '@/lib/session'
import { Loader2 } from 'lucide-react'
import { useRouter, useSearchParams } from 'next/navigation'
import { type FormEvent, useEffect, useState } from 'react'

export function SignIn() {
	const { credentials, signIn } = useSession()
	const router = useRouter()
	const search = useSearchParams()
	const next = safeNextPath(search.get('next'))
	const [token, setToken] = useState('')
	const [tenantId, setTenantId] = useState('')
	const [pending, setPending] = useState(false)
	const [error, setError] = useState<ApiError | null>(null)

	useEffect(() => {
		if (credentials) router.replace(next)
	}, [credentials, next, router])

	async function submit(e: FormEvent) {
		e.preventDefault()
		setError(null)
		setPending(true)
		const candidate = {
			token: token.trim(),
			tenantId: tenantId.trim() || undefined,
		}
		try {
			// Every role (deploy / invoke / operator) may list its own tenant's
			// functions, so this checks the token and the tenant id: 401 is an
			// unknown token, 403 a tenant id that does not match it.
			await apiRequest(candidate, '/v1/functions')
			signIn(candidate)
			setToken('')
		} catch (err) {
			setError(toApiError(err))
		} finally {
			setPending(false)
		}
	}

	return (
		<main className='flex min-h-screen items-center justify-center p-4'>
			<Card className='w-full max-w-md'>
				<CardHeader>
					<CardTitle className='text-xl'>Tachyon Serverless</CardTitle>
					<CardDescription>
						Sign in with a tenant API token. The token stays in this browser tab
						only (session storage) and is sent to this gateway&apos;s{' '}
						<code>/v1</code> API, nowhere else.
					</CardDescription>
				</CardHeader>
				<CardContent>
					<form
						className='flex flex-col gap-4'
						onSubmit={submit}
						data-testid='sign-in-form'
					>
						<div className='flex flex-col gap-2'>
							<Label htmlFor='token'>API token</Label>
							<Input
								id='token'
								name='token'
								type='password'
								autoComplete='off'
								spellCheck={false}
								required
								value={token}
								onChange={e => setToken(e.target.value)}
							/>
						</div>
						<div className='flex flex-col gap-2'>
							<Label htmlFor='tenant'>Tenant id (optional)</Label>
							<Input
								id='tenant'
								name='tenant'
								placeholder='tn_…'
								autoComplete='off'
								spellCheck={false}
								value={tenantId}
								onChange={e => setTenantId(e.target.value)}
							/>
							<p className='text-xs text-muted-foreground'>
								When set, every request carries it and the gateway refuses a
								token of another tenant.
							</p>
						</div>
						{error && (
							<Alert variant='destructive' data-testid='sign-in-error'>
								<AlertTitle>
									{error.status === 401 || error.status === 403
										? 'The token was not accepted'
										: 'Sign-in failed'}
								</AlertTitle>
								<AlertDescription>
									<p>{error.message}</p>
									<ErrorMeta error={error} />
								</AlertDescription>
							</Alert>
						)}
						<Button type='submit' disabled={pending || !token.trim()}>
							{pending && <Loader2 className='animate-spin' />}
							Sign in
						</Button>
					</form>
				</CardContent>
			</Card>
		</main>
	)
}
