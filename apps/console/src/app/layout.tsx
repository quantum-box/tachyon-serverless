import '@tachyon-sdk/native-ui/src/styles/tokens.css'
import './globals.css'
import { Toaster } from '@/components/ui/toaster'
import { SessionProvider } from '@/lib/session'
import type { Metadata } from 'next'
import type { ReactNode } from 'react'

export const metadata: Metadata = {
	title: 'Tachyon Serverless — Functions console',
	description:
		'Deployments, invocation history, logs and provisional usage of Tachyon Serverless functions.',
	referrer: 'no-referrer',
	robots: { index: false, follow: false },
}

export default function RootLayout({ children }: { children: ReactNode }) {
	return (
		<html lang='en'>
			<body className='min-h-screen antialiased'>
				<SessionProvider>{children}</SessionProvider>
				<Toaster />
			</body>
		</html>
	)
}
