// https://eslint.vuejs.org/user-guide/#installation
// https://eslint.org/docs/latest/use/configure/migration-guide#--ext
// https://stackoverflow.com/questions/78348933/how-to-use-eslint-flat-config-for-vue-with-typescript
import js from "@eslint/js";
import eslintPluginVue from "eslint-plugin-vue";
import vueEslintParser from "vue-eslint-parser";

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
        },
        rules: {
            // 'vue/no-unused-vars': 'error'
            //"vue/require-default-prop": "off",
        },
    },
    // `eslintPluginPrettierRecommended` is last to override previous config, and it includes eslintConfigPrettier
    eslintPluginPrettierRecommended,
];
