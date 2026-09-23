import { QueryClient, VueQueryPlugin } from "@tanstack/vue-query";
import { createApp } from "vue";
import { createRouter, createWebHistory } from "vue-router";

import App from "./App.vue";
import "./styles.css";

// Each screen is its own chunk: opening the overview does not download the search page.
const router = createRouter({
    history: createWebHistory(),
    routes: [
        {
            path: "/",
            component: () => import("./views/OverviewView.vue"),
            meta: { title: "Replication" },
        },
        {
            path: "/live",
            component: () => import("./views/LiveView.vue"),
            meta: { title: "Live" },
        },
        {
            path: "/sinks/:id",
            component: () => import("./views/SinkView.vue"),
            props: true,
            meta: { title: "Sink" },
        },
        {
            path: "/search",
            component: () => import("./views/SearchView.vue"),
            meta: { title: "Search" },
        },
        { path: "/:rest(.*)*", redirect: "/" },
    ],
});

router.afterEach((to) => {
    document.title = `${String(to.meta.title ?? "Console")} · Walrus`;
});

// Status changes by the second. Refetching is driven by each screen's own interval, not by window focus.
const queries = new QueryClient({
    defaultOptions: {
        queries: { staleTime: 1_000, refetchOnWindowFocus: false, retry: 1 },
    },
});

createApp(App).use(router).use(VueQueryPlugin, { queryClient: queries }).mount("#app");
