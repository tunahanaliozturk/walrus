<script setup lang="ts">
import { useQuery } from "@tanstack/vue-query";
import { computed, ref } from "vue";

import { api } from "@/api/client";
import QueryState from "@/components/QueryState.vue";
import { useStats } from "@/composables/useStats";

const { stats } = useStats();
const indexes = computed(() => stats.value?.sinks.filter((sink) => sink.kind === "Index") ?? []);

const chosen = ref("");
const text = ref("");

// The first index until someone picks another.
const sink = computed({
    get: () => chosen.value || indexes.value[0]?.id || "",
    set: (id: string) => {
        chosen.value = id;
    },
});

// The query only runs for what was submitted, not for every keystroke.
const submitted = ref<{ sink: string; query: string } | null>(null);

const results = useQuery({
    queryKey: computed(() => ["search", submitted.value?.sink, submitted.value?.query]),
    queryFn: () => api.search(submitted.value!.sink, submitted.value!.query),
    enabled: computed(() => submitted.value !== null),
});

function search() {
    if (sink.value && text.value.trim()) {
        submitted.value = { sink: sink.value, query: text.value.trim() };
    }
}
</script>

<template>
    <div class="page-header">
        <h1>Search</h1>
        <p>
            An index sink holds the captured rows in memory and rebuilds itself from the outbox on
            every start. Every word must match.
        </p>
    </div>

    <p v-if="stats && indexes.length === 0" class="muted">No index sink is registered.</p>

    <form v-else class="search" @submit.prevent="search">
        <div class="field">
            <label for="search-sink">Index</label>
            <select id="search-sink" v-model="sink">
                <option v-for="index in indexes" :key="index.id" :value="index.id">
                    {{ index.id }}
                </option>
            </select>
        </div>
        <div class="field">
            <label for="search-text">Words</label>
            <input id="search-text" v-model="text" type="search" placeholder="owner-42 Paris" />
        </div>
        <button class="button" type="submit">Search</button>
    </form>

    <QueryState :loading="results.isFetching.value" :error="results.error.value" />

    <template v-if="results.data.value">
        <p v-if="results.data.value.rows.length === 0" class="muted">No row contains every word.</p>
        <div v-else class="table-scroll">
            <table>
                <caption>
                    {{
                        results.data.value.rows.length
                    }}
                    rows, ordered by table and key.
                </caption>
                <thead>
                    <tr>
                        <th scope="col">Table</th>
                        <th scope="col">Key</th>
                        <th scope="col">Row</th>
                    </tr>
                </thead>
                <tbody>
                    <tr
                        v-for="hit in results.data.value.rows"
                        :key="`${hit.table}-${hit.key.map((column) => column.value).join('-')}`"
                    >
                        <td>{{ hit.table }}</td>
                        <td>{{ hit.key.map((column) => column.value).join(", ") }}</td>
                        <td>
                            <span v-for="column in hit.row" :key="column.name">
                                <strong>{{ column.name }}</strong>
                                {{ column.value ?? "null" }}&nbsp;
                            </span>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </template>
</template>
