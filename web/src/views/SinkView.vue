<script setup lang="ts">
import { useQuery } from "@tanstack/vue-query";
import { computed } from "vue";

import { api } from "@/api/client";
import QueryState from "@/components/QueryState.vue";
import StateBadge from "@/components/StateBadge.vue";
import { useStats } from "@/composables/useStats";
import { count, duration } from "@/format";

const props = defineProps<{ id: string }>();

const { stats, error, isPending } = useStats();
const sink = computed(() => stats.value?.sinks.find((candidate) => candidate.id === props.id));

const letters = useQuery({
    queryKey: computed(() => ["dead-letters", props.id]),
    queryFn: () => api.deadLetters(props.id),
    refetchInterval: 5_000,
});

const columns = (values: { name: string; value: string | null }[]) =>
    values.map((column) => `${column.name}=${column.value ?? "null"}`).join(", ");
</script>

<template>
    <div class="page-header">
        <h1>Sink {{ id }}</h1>
        <p v-if="sink">{{ sink.kind }} sink receiving {{ sink.tables.join(", ") }}.</p>
    </div>

    <QueryState :loading="isPending" :error="error" />
    <p v-if="stats && !sink" class="state state--error" role="alert">
        No sink is registered as {{ id }}.
    </p>

    <template v-if="sink">
        <dl class="facts card">
            <dt>State</dt>
            <dd><StateBadge :state="sink.state" /></dd>
            <dt>Target</dt>
            <dd>{{ sink.target ?? "held in memory" }}</dd>
            <dt>Rows held behind a dead letter</dt>
            <dd>{{ count(sink.blockedRows) }}</dd>
            <dt>Batches retried</dt>
            <dd>{{ count(sink.counters.retries) }}</dd>
            <template v-if="sink.counters.lastError">
                <dt>Last failure</dt>
                <dd class="state--error">{{ sink.counters.lastError }}</dd>
            </template>
        </dl>

        <h2>Position in each source</h2>
        <div class="table-scroll">
            <table>
                <thead>
                    <tr>
                        <th scope="col">Source</th>
                        <th scope="col" class="number">Applied up to</th>
                        <th scope="col" class="number">Outbox head</th>
                        <th scope="col">Started at</th>
                        <th scope="col">Oldest pending commit</th>
                        <th scope="col" class="number">Lag</th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="source in sink.sources" :key="source.source">
                        <th scope="row">{{ source.source }}</th>
                        <td class="number">{{ count(source.appliedSeq) }}</td>
                        <td class="number">{{ count(source.headSeq) }}</td>
                        <td>
                            <code>{{ source.startLsn }}</code>
                        </td>
                        <td>
                            <code>{{ source.pendingLsn ?? "caught up" }}</code>
                        </td>
                        <td class="number">{{ duration(source.lagMilliseconds) }}</td>
                    </tr>
                </tbody>
            </table>
        </div>

        <h2>Dead letters</h2>
        <QueryState :loading="letters.isPending.value" :error="letters.error.value" />
        <p v-if="letters.data.value?.length === 0" class="muted">
            None. Every change this sink received has been applied.
        </p>
        <div v-else-if="letters.data.value" class="table-scroll">
            <table>
                <caption>
                    Retrying them is an operator action, through the API with the operator token.
                </caption>
                <thead>
                    <tr>
                        <th scope="col">Parked</th>
                        <th scope="col">Source</th>
                        <th scope="col">Table</th>
                        <th scope="col">Operation</th>
                        <th scope="col">Row</th>
                        <th scope="col" class="number">Attempts</th>
                        <th scope="col">Why</th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="letter in letters.data.value" :key="letter.id">
                        <td>{{ new Date(letter.deadAt).toLocaleString("en-GB") }}</td>
                        <td>{{ letter.source }}</td>
                        <td>{{ letter.table }}</td>
                        <td>{{ letter.operation }}</td>
                        <td>{{ columns(letter.key) }}</td>
                        <td class="number">{{ letter.attempts }}</td>
                        <td>{{ letter.error }}</td>
                    </tr>
                </tbody>
            </table>
        </div>
    </template>
</template>
