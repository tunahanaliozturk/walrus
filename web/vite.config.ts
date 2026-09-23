import { fileURLToPath, URL } from "node:url";

import vue from "@vitejs/plugin-vue";
import { defineConfig } from "vitest/config";

// The browser never holds a token. In development this proxy adds the read token on the way to the API, and in
// the container nginx does the same, so the console is a read-only window and nothing in the bundle, the page
// or the browser's storage is a credential.
const api = process.env.WALRUS_API ?? "http://127.0.0.1:5190";
const readToken = process.env.WALRUS_READ_TOKEN ?? "walrus-read-token-for-local-use-only";

export default defineConfig({
    plugins: [vue()],
    resolve: {
        alias: {
            "@": fileURLToPath(new URL("./src", import.meta.url)),
        },
    },
    server: {
        proxy: {
            "/v1": {
                target: api,
                changeOrigin: true,
                headers: { Authorization: `Bearer ${readToken}` },
            },
        },
    },
    test: {
        environment: "jsdom",
        globals: true,
        include: ["src/**/*.test.ts"],
        coverage: {
            provider: "v8",
            reporter: ["text", "lcov"],
            include: ["src/**/*.{ts,vue}"],
            exclude: ["src/api/schema.d.ts", "src/main.ts", "src/**/*.test.ts"],
        },
    },
    build: {
        // The budget script reads the manifest to work out what a first visit actually costs.
        manifest: true,
        chunkSizeWarningLimit: 160,
    },
});
