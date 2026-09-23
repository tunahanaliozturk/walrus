<script setup lang="ts">
import { useQuery } from "@tanstack/vue-query";
import { computed, ref } from "vue";

import { api } from "@/api/client";
import type { FeedEntry } from "@/api/contract";
import QueryState from "@/components/QueryState.vue";
import { useFeed } from "@/composables/useFeed";
import { key } from "@/format";

type Filter = "All" | FeedEntry["kind"];

const filters: { value: Filter; label: string }[] = [
    { value: "All", label: "Everything" },
    { value: "Change", label: "Captured changes" },
    { value: "Conflict", label: "Conflicts" },
    { value: "DeadLetter", label: "Dead letters" },
];

const { entries, status, rejected } = useFeed();

// Conflicts that happened before the page opened, so the conflict tab is never empty just because nothing
// has conflicted in the last few seconds.
const earlier = useQuery({ queryKey: ["conflicts"], queryFn: api.conflicts });

const filter = ref<Filter>("All");

const shown = computed(() => {
    const live = entries.value;
    const history = filter.value === "Conflict" ? [...(earlier.data.value ?? [])].reverse() : [];
    const all = [
        ...live,
        ...history.filter(
            (entry) => !live.some((seen) => seen.at === entry.at && seen.key === entry.key),
        ),
    ];

    return filter.value === "All" ? all : all.filter((entry) => entry.kind === filter.value);
});

const time = (instant: string) => new Date(instant).toLocaleTimeString("en-GB");

const kindLabel: Record<FeedEntry["kind"], string> = {
    Change: "change",
    Conflict: "conflict",
    DeadLetter: "dead letter",
};

const kindTone: Record<FeedEntry["kind"], string> = {
    Change: "badge--muted",
    Conflict: "badge--warn",
    DeadLetter: "badge--danger",
};
</script>

<template>
    <div class="page-header">
        <h1>Live</h1>
        <p>
            Captured changes as they reach the outbox, sampled to a few a second per source, and
            every conflict decision and dead letter as it happens.
            <span role="status">Stream {{ status }}.</span>
            <span v-if="rejected > 0"> {{ rejected }} malformed events were skipped.</span>
        </p>
    </div>

    <div class="tabs" role="group" aria-label="Show">
        <button
            v-for="option in filters"
            :key="option.value"
            type="button"
            class="tab"
            :aria-pressed="filter === option.value"
            @click="filter = option.value"
        >
            {{ option.label }}
        </button>
    </div>

    <QueryState :loading="false" :error="earlier.error.value" />

    <p v-if="shown.length === 0" class="muted">
        Nothing yet. Write to a source and it appears here.
    </p>
    <ol v-else class="feed" aria-label="Events, newest first">
        <li
            v-for="entry in shown"
            :key="`${entry.kind}-${entry.at}-${entry.source}-${entry.key}-${entry.sink}`"
        >
            <span>{{ time(entry.at) }}</span>
            <span
                ><span class="badge" :class="kindTone[entry.kind]">{{
                    kindLabel[entry.kind]
                }}</span></span
            >
            <span>{{ entry.source }}</span>
            <span>{{ entry.table }}</span>
            <span>{{ entry.operation }}</span>
            <span class="feed__detail">
                {{ key(entry.key) }}
                <template v-if="entry.sink"> in {{ entry.sink }}</template>
                <template v-if="entry.detail">: {{ entry.detail }}</template>
            </span>
        </li>
    </ol>
</template>
