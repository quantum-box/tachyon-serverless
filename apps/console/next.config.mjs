// Functions console (PLT-4644).
//
// `next build` writes a static export to `out/`, served by the gateway under
// `/console/` when `[console] enabled = true` (docs/console.md). There is no
// Next.js server in production: no API routes, no server actions, no
// middleware. Every API call is made by the browser to the same origin with
// the viewer's tenant token.
//
// `next dev` serves the same pages at http://127.0.0.1:3100/console/ and
// proxies `/v1/*` to the gateway named by TSLS_API_URL (default
// http://127.0.0.1:8080), so the browser still talks to one origin.

const isDev = process.env.NODE_ENV === 'development'
const apiUrl = process.env.TSLS_API_URL ?? 'http://127.0.0.1:8080'

/** @type {import('next').NextConfig} */
const nextConfig = {
	basePath: '/console',
	trailingSlash: true,
	reactStrictMode: true,
	poweredByHeader: false,
	// native-ui is consumed as TypeScript source (like apps/tachyon does).
	transpilePackages: ['@tachyon-sdk/native-ui'],
	images: { unoptimized: true },
	...(isDev
		? {
				async rewrites() {
					return [
						{
							source: '/v1/:path*',
							destination: `${apiUrl}/v1/:path*`,
							basePath: false,
						},
					]
				},
			}
		: { output: 'export' }),
}

export default nextConfig
