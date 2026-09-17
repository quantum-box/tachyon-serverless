import { Suspense } from 'react'
import { SignIn } from './sign-in'

export default function Page() {
	return (
		<Suspense>
			<SignIn />
		</Suspense>
	)
}
