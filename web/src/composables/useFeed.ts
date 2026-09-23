import { onScopeDispose, ref, shallowRef } from "vue";

import { FeedEntry } from "@/api/contract";

/** How many entries the live page keeps. */
export const FeedLength = 200;

export type FeedStatus = "connecting" | "open" | "reconnecting";

/**
 * The live event stream. The browser's EventSource reconnects on its own after a drop, so the only state here is
 * the entries and whether the stream is currently open. Each message is parsed like any other response: a
 * malformed one is skipped and counted, not rendered.
 */
export function useFeed(source: () => EventSource = () => new EventSource("/v1/events")) {
    const entries = shallowRef<FeedEntry[]>([]);
    const status = ref<FeedStatus>("connecting");
    const rejected = ref(0);
    const events = source();

    events.onopen = () => {
        status.value = "open";
    };

    events.onerror = () => {
        status.value = "reconnecting";
    };

    events.onmessage = (message: MessageEvent<string>) => {
        let parsed: unknown;

        try {
            parsed = JSON.parse(message.data);
        } catch {
            rejected.value++;
            return;
        }

        const entry = FeedEntry.safeParse(parsed);

        if (!entry.success) {
            rejected.value++;
            return;
        }

        entries.value = [entry.data, ...entries.value].slice(0, FeedLength);
    };

    // A composable may run outside a component, and a scope is what it always has.
    onScopeDispose(() => events.close());

    return { entries, status, rejected };
}
