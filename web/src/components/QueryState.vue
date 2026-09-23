<script setup lang="ts">
import { computed } from "vue";

const props = defineProps<{ loading: boolean; error: unknown }>();

const message = computed(() =>
    props.error instanceof Error ? props.error.message : "Something went wrong.",
);
</script>

<template>
    <p v-if="loading" class="state" role="status">Loading…</p>
    <p v-else-if="error" class="state state--error" role="alert">{{ message }}</p>
</template>
