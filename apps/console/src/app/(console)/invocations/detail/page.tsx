'use client'

import { LoadingState } from '@/components/functions/data-state'
import { InvocationDetail } from '@/components/functions/invocation-detail'
import { Suspense } from 'react'

export default function InvocationDetailPage() {
	return (
		<Suspense fallback={<LoadingState />}>
			<InvocationDetail />
		</Suspense>
	)
}
