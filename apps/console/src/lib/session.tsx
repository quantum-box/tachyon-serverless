'use client'

// Tenant token session of the console.
//
// The token lives in React state and in `sessionStorage` of this tab only
// (cleared when the tab closes or on sign-out). It is never written to
// `localStorage`, a cookie, a URL or the console log. In the Tachyon Console
// this adapter is replaced by the next-auth session (docs/console-integration.md).

import {
	type ReactNode,
	createContext,
	useCallback,
	useContext,
	useEffect,
	useMemo,
	useState,
} from 'react'
import type { Credentials } from './serverless-api/client'

const STORAGE_KEY = 'tsls.console.session.v1'

export interface SessionValue {
	/** `undefined` until the stored session was read on the client. */
	credentials: Credentials | null | undefined
	signIn: (credentials: Credentials) => void
	signOut: () => void
}

const SessionContext = createContext<SessionValue | null>(null)

function readStored(): Credentials | null {
	try {
		const raw = window.sessionStorage.getItem(STORAGE_KEY)
		if (!raw) return null
		const v = JSON.parse(raw) as Partial<Credentials>
		if (typeof v.token !== 'string' || !v.token) return null
		return {
			token: v.token,
			tenantId:
				typeof v.tenantId === 'string' && v.tenantId ? v.tenantId : undefined,
		}
	} catch {
		return null
	}
}

export function SessionProvider({ children }: { children: ReactNode }) {
	const [credentials, setCredentials] = useState<
		Credentials | null | undefined
	>(undefined)

	useEffect(() => {
		setCredentials(readStored())
	}, [])

	const signIn = useCallback((c: Credentials) => {
		const clean: Credentials = {
			token: c.token.trim(),
			tenantId: c.tenantId?.trim() || undefined,
		}
		try {
			window.sessionStorage.setItem(STORAGE_KEY, JSON.stringify(clean))
		} catch {
			// Storage unavailable (private mode): keep the session in memory only.
		}
		setCredentials(clean)
	}, [])

	const signOut = useCallback(() => {
		try {
			window.sessionStorage.removeItem(STORAGE_KEY)
		} catch {
			// nothing stored
		}
		setCredentials(null)
	}, [])

	const value = useMemo(
		() => ({ credentials, signIn, signOut }),
		[credentials, signIn, signOut],
	)
	return (
		<SessionContext.Provider value={value}>{children}</SessionContext.Provider>
	)
}

export function useSession(): SessionValue {
	const v = useContext(SessionContext)
	if (!v) throw new Error('useSession outside SessionProvider')
	return v
}

/** Credentials of a page rendered inside the signed-in shell. */
export function useCredentials(): Credentials {
	const { credentials } = useSession()
	if (!credentials) throw new Error('useCredentials without a session')
	return credentials
}
