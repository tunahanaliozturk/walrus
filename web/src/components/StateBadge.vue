<script setup lang="ts">
import { computed } from "vue";

import type { Sink } from "@/api/contract";

const props = defineProps<{ state: Sink["state"] }>();

// Words carry the meaning; colour only repeats it.
const tone = computed(
    () =>
        ({
            Running: "badge--ok",
            Retrying: "badge--danger",
            Rebuilding: "badge--warn",
            Stopped: "badge--muted",
        })[props.state],
);
</script>

<template>
    <span class="badge" :class="tone">{{ state }}</span>
</template>
