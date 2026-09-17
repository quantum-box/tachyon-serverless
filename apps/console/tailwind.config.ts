import nativeUiPreset from '@tachyon-sdk/native-ui/src/tailwind-preset'
import type { Config } from 'tailwindcss'

// Same setup as quantum-box/tachyon-apps apps/tachyon/tailwind.config.ts:
// the Tachyon Native UI preset supplies every semantic color, radius and the
// type scale. No console-specific design tokens are added here.
const config = {
	presets: [nativeUiPreset],
	darkMode: ['class'],
	content: [
		'./src/**/*.{ts,tsx}',
		// native-ui is consumed as source; scan it so its classes compile
		'./node_modules/@tachyon-sdk/native-ui/src/**/*.{ts,tsx}',
	],
	prefix: '',
	theme: {
		container: {
			center: true,
			padding: '2rem',
			screens: {
				'2xl': '1400px',
			},
		},
	},
} satisfies Config

export default config
