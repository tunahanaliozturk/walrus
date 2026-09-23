import { useQuery } from "@tanstack/vue-query";
import { shallowRef, watch, type ShallowRef } from "vue";

import { api } from "@/api/client";
import type { Stats } from "@/api/contract";

/** How many samples of each sink's lag the sparklines keep: two minutes at one sample every two seconds. */
export const HistoryLength = 60;

export interface Throughput {
    /** Changes captured per second, by source. */
    captured: Record<string, number>;
    /** Changes delivered per second, by sink. */
    delivered: Record<string, number>;
}

/** The slowest source's lag for a sink, which is the lag that matters. */
export function sinkLag(sink: Stats["sinks"][number]): number {
    return sink.sources.reduce((worst, source) => Math.max(worst, source.lagMilliseconds), 0);
}

/**
 * Differences two samples of the running totals into per-second rates. Totals only grow while the service
 * runs, so a total that went down means the service restarted, and that interval reports zero rather than
 * a negative rate.
 */
export function throughput(previous: Stats, current: Stats): Throughput {
    const seconds = (Date.parse(current.at) - Date.parse(previous.at)) / 1_000;
    const per = (now: number, before: number | undefined) =>
        seconds > 0 && before !== undefined && now >= before ? (now - before) / seconds : 0;

    return {
        captured: Object.fromEntries(
            current.sources.map((source) => [
                source.name,
                per(
                    source.changes,
                    previous.sources.find((old) => old.name === source.name)?.changes,
                ),
            ]),
        ),
        delivered: Object.fromEntries(
            current.sinks.map((sink) => {
                const old = previous.sinks.find((candidate) => candidate.id === sink.id);
                return [
                    sink.id,
                    per(
                        sink.counters.changed + sink.counters.unchanged,
                        old ? old.counters.changed + old.counters.unchanged : undefined,
                    ),
                ];
            }),
        ),
    };
}

/**
 * The stats endpoint, polled every two seconds, with the rates and lag history the page draws. The history is
 * an effect of each new sample arriving, which is why this is a watch and not a computed: it accumulates.
 */
export function useStats() {
    const query = useQuery({ queryKey: ["stats"], queryFn: api.stats, refetchInterval: 2_000 });
    const rates: ShallowRef<Throughput> = shallowRef({ captured: {}, delivered: {} });
    const lagHistory: ShallowRef<Record<string, number[]>> = shallowRef({});
    let previous: Stats | undefined;

    watch(
        () => query.data.value,
        (stats) => {
            if (!stats) {
                return;
            }

            if (previous && previous.at !== stats.at) {
                rates.value = throughput(previous, stats);
            }

            const history: Record<string, number[]> = {};

            for (const sink of stats.sinks) {
                history[sink.id] = [...(lagHistory.value[sink.id] ?? []), sinkLag(sink)].slice(
                    -HistoryLength,
                );
            }

            lagHistory.value = history;
            previous = stats;
        },
        { immediate: true },
    );

    return { stats: query.data, error: query.error, isPending: query.isPending, rates, lagHistory };
}
