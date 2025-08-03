// https://vitest.dev/config/

import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
	// https://vitest.dev/config/#include
	include: ['unit-tests/**/*.{test,spec}.js'],
	alias: {
		// similare to `base` in `vite.config.js`
	  '@/': '../src/main/resources/static/ui/', 
	}
  },
})