<script setup lang="ts">
import { computed } from "vue";

import QueryState from "@/components/QueryState.vue";
import Sparkline from "@/components/Sparkline.vue";
import StateBadge from "@/components/StateBadge.vue";
import { sinkLag, useStats } from "@/composables/useStats";
import { ago, bytes, count, duration, rate } from "@/format";

const { stats, error, isPending, rates, lagHistory } = useStats();

// A source can fail over only if every standby holds a synchronised copy of the capture slot.
const failoverReady = (standbys: { synced: boolean }[]) =>
    standbys.length > 0 && standbys.every((standby) => standby.synced);

const summary = computed(() => {
    if (!stats.value) {
        return "";
    }

    const captured = Object.values(rates.value.captured).reduce((total, value) => total + value, 0);
    const worst = stats.value.sinks.reduce((highest, sink) => Math.max(highest, sinkLag(sink)), 0);

    return `${stats.value.sources.length} sources capturing ${rate(captured)}, ${stats.value.sinks.length} sinks, slowest sink ${duration(worst)} behind.`;
});
</script>

<template>
    <div class="page-header">
        <h1>Replication</h1>
        <p>{{ summary || "Every source Walrus captures, and every sink it feeds." }}</p>
    </div>

    <QueryState :loading="isPending" :error="error" />

    <template v-if="stats">
        <section aria-labelledby="sources">
            <h2 id="sources">Sources</h2>
            <div class="cards">
                <article v-for="source in stats.sources" :key="source.name" class="card">
                    <h3>
                        {{ source.name }}
                        <span v-if="source.attachedTo" class="badge badge--ok">Capturing</span>
                        <span v-else class="badge badge--danger">Reconnecting</span>
                    </h3>
                    <dl class="facts">
                        <dt>Streaming from</dt>
                        <dd>{{ source.attachedTo ?? "nothing" }}</dd>
                        <dt>Captured</dt>
                        <dd>
                            {{ count(source.changes) }} changes,
                            {{ rate(rates.captured[source.name] ?? 0) }}
                        </dd>
                        <dt>Last commit</dt>
                        <dd>{{ ago(source.lastCommit) }}</dd>
                        <dt>Log kept for the slot</dt>
                        <dd>{{ bytes(source.walRetainedBytes) }}</dd>
                        <dt>Resent after restarts</dt>
                        <dd>{{ count(source.resent) }} transactions, skipped</dd>
                        <dt>Failover</dt>
                        <dd>
                            <span v-if="source.standbys.length === 0" class="muted"
                                >single node</span
                            >
                            <span v-else-if="failoverReady(source.standbys)" class="badge badge--ok"
                                >Ready: slot synchronised to
                                {{
                                    source.standbys.map((standby) => standby.host).join(", ")
                                }}</span
                            >
                            <span v-else class="badge badge--warn"
                                >Not ready: a standby has no synchronised slot</span
                            >
                        </dd>
                        <template v-if="source.captureIsSynchronous">
                            <dt>Commits</dt>
                            <dd>
                                <span class="badge badge--danger"
                                    >Every commit waits for Walrus</span
                                >
                                The source counts capture as a synchronous standby. Name the
                                physical standbys in synchronous_standby_names instead of *.
                            </dd>
                        </template>
                        <template v-if="source.lastError">
                            <dt>Last error</dt>
                            <dd class="state--error">{{ source.lastError }}</dd>
                        </template>
                    </dl>
                </article>
            </div>
        </section>

        <section aria-labelledby="sinks">
            <h2 id="sinks">Sinks</h2>
            <p v-if="stats.sinks.length === 0" class="muted">No sinks are registered yet.</p>
            <div v-else class="table-scroll">
                <table>
                    <caption>
                        Lag is how long ago the oldest change a sink has not applied committed at
                        its source.
                    </caption>
                    <thead>
                        <tr>
                            <th scope="col">Sink</th>
                            <th scope="col">Kind</th>
                            <th scope="col">State</th>
                            <th scope="col" class="number">Lag</th>
                            <th scope="col" class="number">Delivered</th>
                            <th scope="col" class="number">Changed</th>
                            <th scope="col" class="number">Already applied</th>
                            <th scope="col" class="number">Conflicts lost</th>
                            <th scope="col" class="number">Dead letters</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr v-for="sink in stats.sinks" :key="sink.id">
                            <th scope="row">
                                <RouterLink :to="`/sinks/${sink.id}`">{{ sink.id }}</RouterLink>
                            </th>
                            <td>{{ sink.kind }}</td>
                            <td><StateBadge :state="sink.state" /></td>
                            <td class="number">
                                {{ duration(sinkLag(sink)) }}
                                <Sparkline :values="lagHistory[sink.id] ?? []" />
                            </td>
                            <td class="number">{{ rate(rates.delivered[sink.id] ?? 0) }}</td>
                            <td class="number">{{ count(sink.counters.changed) }}</td>
                            <td class="number">{{ count(sink.counters.unchanged) }}</td>
                            <td class="number">{{ count(sink.counters.conflicts) }}</td>
                            <td class="number">
                                {{ count(sink.counters.deadLetters) }}
                                <span v-if="sink.blockedRows > 0" class="badge badge--danger"
                                    >{{ count(sink.blockedRows) }} rows held</span
                                >
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </section>
    </template>
</template>
