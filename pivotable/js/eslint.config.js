// https://eslint.vuejs.org/user-guide/#installation
// https://eslint.org/docs/latest/use/configure/migration-guide#--ext
// https://stackoverflow.com/questions/78348933/how-to-use-eslint-flat-config-for-vue-with-typescript
import js from "@eslint/js";
import eslintPluginVue from "eslint-plugin-vue";
import vueEslintParser from "vue-eslint-parser";
import globals from "globals";

// https://github.com/prettier/eslint-config-prettier
// `eslint-plugin-prettier` will apply Prettier through ESLint
import eslintPluginPrettierRecommended from "eslint-plugin-prettier/recommended";

// About HTML templates in .js files:
// We prefix the html block with `/* HTML */`, which shall be detected by some VSCode extension (// https://marketplace.visualstudio.com/items?itemName=Tobermory.es6-string-html)
// And by Prettier (https://github.com/prettier/prettier/issues/12957). It is also referred in https://fr.vuejs.org/guide/quick-start

export default [
	js.configs.recommended,
	...eslintPluginVue.configs["flat/recommended"],
	{
		files: ["*.html", "**/*.html", "*.js", "**/*.js", "*.vue", "**/*.vue"],
		// ignores: ["*/target/*"],
		languageOptions: {
			parser: vueEslintParser,
			// Declare the SPA runs in the browser so ESLint treats `window`, `document`,
			// `history`, `location`, `localStorage`, … as known globals. Required by the
			// `no-shadow` rule below (with `builtinGlobals: true`) to recognise that a
			// local `const history` shadows a browser global. Without this `globals`
			// declaration the rule has nothing to compare a local name against.
			globals: { ...globals.browser },
		},
		rules: {
			// 'vue/no-unused-vars': 'error'
			//"vue/require-default-prop": "off",

			// Flag local variables that shadow a built-in browser global (`history`, `location`,
			// `event`, `name`, `parent`, …). Catches the class of bug where naming a local
			// `history` inside a Vue setup() silently breaks every `history.pushState(...)`
			// call in the same scope — exactly the mistake that crashed the URL-hash watcher
			// in adhoc-query.js (see the matching note above `const queryHistory = ...`).
			//
			// `builtinGlobals: true` is the load-bearing flag: without it `no-shadow` only
			// flags shadowing of OUTER LEXICAL variables (which the recommended preset already
			// catches indirectly via `no-unused-vars`), not browser globals.
			//
			// `hoist: "all"` makes the rule hoist-aware so a `const x` near the top of a block
			// is also flagged when an inner block declares its own `x`. Lower-noise default
			// would be `"functions"` (hoist function decls only); `"all"` keeps us strict.
			"no-shadow": ["error", { builtinGlobals: true, hoist: "all" }],
		},
	},
	// `eslintPluginPrettierRecommended` is last to override previous config, and it includes eslintConfigPrettier
	eslintPluginPrettierRecommended,
];
