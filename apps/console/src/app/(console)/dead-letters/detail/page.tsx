'use client'

import { LoadingState } from '@/components/functions/data-state'
import { DeadLetterDetail } from '@/components/functions/dead-letter-detail'
import { Suspense } from 'react'

export default function DeadLetterDetailPage() {
	return (
		<Suspense fallback={<LoadingState />}>
			<DeadLetterDetail />
		</Suspense>
	)
}
