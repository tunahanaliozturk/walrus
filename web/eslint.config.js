import vue from "eslint-plugin-vue";
import accessibility from "eslint-plugin-vuejs-accessibility";
import typescript from "typescript-eslint";

export default typescript.config(
    { ignores: ["dist", "coverage", "playwright-report", "test-results", "src/api/schema.d.ts"] },
    ...typescript.configs.recommended,
    ...vue.configs["flat/recommended"],
    ...accessibility.configs["flat/recommended"],
    {
        files: ["**/*.vue"],
        languageOptions: {
            parserOptions: { parser: typescript.parser },
        },
    },
    {
        rules: {
            "@typescript-eslint/no-explicit-any": "error",
            "@typescript-eslint/consistent-type-imports": "error",
            "vue/multi-word-component-names": "off",

            // Formatting belongs to Prettier. Two tools disagreeing about line breaks produce a lint run
            // nobody reads.
            "vue/singleline-html-element-content-newline": "off",
            "vue/multiline-html-element-content-newline": "off",
            "vue/max-attributes-per-line": "off",
            "vue/html-self-closing": "off",
            "vue/html-indent": "off",
            "vue/html-closing-bracket-newline": "off",
            "vue/attributes-order": "off",

            // A label associated by for/id is complete; demanding it also wrap the control is ceremony.
            "vuejs-accessibility/label-has-for": [
                "error",
                { required: { some: ["nesting", "id"] }, allowChildren: false },
            ],
        },
    },
);
