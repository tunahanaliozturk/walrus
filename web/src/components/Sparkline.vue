<script setup lang="ts">
import { computed } from "vue";

import { duration } from "@/format";

const props = withDefaults(defineProps<{ values: number[]; width?: number; height?: number }>(), {
    width: 96,
    height: 24,
});

// Scaled to the highest value shown, with a floor, so a flat line at a few milliseconds reads as flat and
// not as a cliff.
const points = computed(() => {
    const peak = Math.max(50, ...props.values);
    const step = props.values.length > 1 ? props.width / (props.values.length - 1) : 0;

    return props.values
        .map((value, index) => {
            const x = (index * step).toFixed(1);
            const y = (props.height - 1 - (value / peak) * (props.height - 2)).toFixed(1);
            return `${x},${y}`;
        })
        .join(" ");
});

const label = computed(() => {
    if (props.values.length === 0) {
        return "No lag measured yet";
    }

    const latest = props.values[props.values.length - 1] ?? 0;
    return `Lag over the last ${props.values.length * 2} seconds: now ${duration(latest)}, highest ${duration(Math.max(...props.values))}`;
});
</script>

<template>
    <svg
        class="sparkline"
        role="img"
        :aria-label="label"
        :width="width"
        :height="height"
        :viewBox="`0 0 ${width} ${height}`"
    >
        <polyline v-if="values.length > 1" :points="points" />
    </svg>
</template>
