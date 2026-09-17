'use client'

import { FunctionDetail } from '@/components/functions/function-detail'
import { LoadingState } from '@/components/functions/data-state'
import { Suspense } from 'react'

export default function FunctionDetailPage() {
	return (
		<Suspense fallback={<LoadingState />}>
			<FunctionDetail />
		</Suspense>
	)
}
