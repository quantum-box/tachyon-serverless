export function formatDateTime(value: string | null | undefined): string {
	if (!value) return '—'
	const d = new Date(value)
	if (Number.isNaN(d.getTime())) return value
	return d
		.toISOString()
		.replace('T', ' ')
		.replace(/\.\d+Z$/, 'Z')
}

export function formatMs(value: number | null | undefined): string {
	if (value === null || value === undefined) return '—'
	if (value < 1000) return `${value} ms`
	return `${(value / 1000).toFixed(2)} s`
}

export function formatBytes(value: number | null | undefined): string {
	if (value === null || value === undefined) return '—'
	if (value < 1024) return `${value} B`
	if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`
	return `${(value / 1024 / 1024).toFixed(1)} MiB`
}

/**
 * Provisional charges are integer micro-units of the price table currency
 * (docs/api.md §5.10.1). Shown with 6 decimals so nothing is rounded away.
 */
export function formatMicros(
	micros: number | null | undefined,
	currency: string,
): string {
	if (micros === null || micros === undefined) return '—'
	const sign = micros < 0 ? '-' : ''
	const abs = Math.abs(micros)
	const whole = Math.floor(abs / 1_000_000)
	const frac = String(abs % 1_000_000).padStart(6, '0')
	return `${sign}${whole}.${frac} ${currency}`
}

/** Short form of a prefixed ULID id (`inv_01j7…w2`) for dense tables. */
export function shortId(id: string | null | undefined): string {
	if (!id) return '—'
	if (id.length <= 14) return id
	return `${id.slice(0, 8)}…${id.slice(-4)}`
}

/** Human number with thousands separators. */
export function formatCount(value: number | null | undefined): string {
	if (value === null || value === undefined) return '—'
	return value.toLocaleString('en-US')
}
