<script setup lang="ts">
const links = [
    { to: "/", label: "Replication" },
    { to: "/live", label: "Live" },
    { to: "/search", label: "Search" },
];
</script>

<template>
    <a class="skip-link" href="#main">Skip to content</a>
    <header class="topbar">
        <span class="brand">
            <img src="/icon.svg" alt="" width="24" height="24" />
            Walrus
        </span>
        <nav aria-label="Main">
            <RouterLink
                v-for="link in links"
                :key="link.to"
                :to="link.to"
                class="nav-link"
                exact-active-class="router-link-active"
                >{{ link.label }}</RouterLink
            >
        </nav>
    </header>
    <main id="main" tabindex="-1">
        <RouterView />
    </main>
</template>
