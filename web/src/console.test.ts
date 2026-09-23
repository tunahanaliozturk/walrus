import { render, screen } from "@testing-library/vue";
import { describe, expect, it } from "vitest";
import { effectScope } from "vue";

import { Stats, type FeedEntry } from "@/api/contract";
import Sparkline from "@/components/Sparkline.vue";
import StateBadge from "@/components/StateBadge.vue";
import { useFeed } from "@/composables/useFeed";
import { sinkLag, throughput } from "@/composables/useStats";
import { ago, bytes, duration, key, rate } from "@/format";

const stats = (at: string, captured: number, delivered: number, lag = [12, 340]) =>
    Stats.parse({
        at,
        sources: [
            {
                name: "eu",
                attachedTo: "pg-a:5432",
                primary: "pg-a:5432",
                walRetainedBytes: 2048,
                confirmedFlushLsn: "0/3000000",
                slotActive: true,
                standbys: [{ host: "pg-b:5432", synced: true, confirmedFlushLsn: "0/3000000" }],
                changes: captured,
                transactions: captured,
                resent: 0,
                lastCommit: at,
                lastError: null,
                checkedAt: at,
            },
        ],
        sinks: [
            {
                id: "replica",
                kind: "Postgres",
                tables: ["public.accounts"],
                target: "replica",
                state: "Running",
                blockedRows: 0,
                sources: lag.map((lagMilliseconds, index) => ({
                    source: index === 0 ? "eu" : "us",
                    appliedSeq: 10,
                    headSeq: 12,
                    startLsn: "0/0",
                    pendingLsn: null,
                    lagMilliseconds,
                })),
                counters: {
                    changed: delivered,
                    unchanged: 0,
                    conflicts: 0,
                    deadLetters: 0,
                    retries: 0,
                    lastError: null,
                },
            },
        ],
    });

describe("the contract", () => {
    it("parses a stats response", () => {
        expect(stats("2026-09-23T12:00:00Z", 10, 5).sinks[0]?.kind).toBe("Postgres");
    });

    it("refuses a response that does not match, rather than rendering half of it", () => {
        expect(() => Stats.parse({ at: "now", sources: [{ name: "eu" }], sinks: [] })).toThrow();
    });

    it("refuses a sink state the console does not know", () => {
        const response = stats("2026-09-23T12:00:00Z", 1, 1);
        expect(() =>
            Stats.parse({ ...response, sinks: [{ ...response.sinks[0], state: "Melting" }] }),
        ).toThrow();
    });
});

describe("rates and lag", () => {
    it("differences two samples into rates per second", () => {
        const rates = throughput(
            stats("2026-09-23T12:00:00Z", 1_000, 400),
            stats("2026-09-23T12:00:02Z", 7_000, 2_400),
        );

        expect(rates.captured.eu).toBe(3_000);
        expect(rates.delivered.replica).toBe(1_000);
    });

    it("reports zero, not a negative rate, across a restart", () => {
        const rates = throughput(
            stats("2026-09-23T12:00:00Z", 5_000, 5_000),
            stats("2026-09-23T12:00:02Z", 10, 10),
        );

        expect(rates.captured.eu).toBe(0);
    });

    it("takes the slowest source as a sink's lag", () => {
        expect(sinkLag(stats("2026-09-23T12:00:00Z", 1, 1).sinks[0]!)).toBe(340);
    });
});

describe("formatting", () => {
    it("formats durations, rates, bytes and keys for people", () => {
        expect(duration(12.4)).toBe("12 ms");
        expect(duration(1_540)).toBe("1.5 s");
        expect(rate(3_000)).toBe("3,000/s");
        expect(rate(2.5)).toBe("2.5/s");
        expect(bytes(1_536)).toBe("1.5 KiB");
        expect(bytes(null)).toBe("unknown");
        expect(key('{"id":"7","region":"eu"}')).toBe("id=7, region=eu");
        expect(ago("2026-09-23T12:00:00Z", Date.parse("2026-09-23T12:02:00Z"))).toBe("2 min ago");
    });
});

describe("components", () => {
    it("describes a lag sparkline in words", () => {
        render(Sparkline, { props: { values: [10, 40, 25] } });

        expect(screen.getByRole("img").getAttribute("aria-label")).toBe(
            "Lag over the last 6 seconds: now 25 ms, highest 40 ms",
        );
    });

    it("names a sink's state in words, not only colour", () => {
        render(StateBadge, { props: { state: "Retrying" } });

        expect(screen.getByText("Retrying")).toBeTruthy();
    });
});

class FakeEventSource {
    onopen: (() => void) | null = null;
    onerror: (() => void) | null = null;
    onmessage: ((message: MessageEvent<string>) => void) | null = null;
    closed = false;

    close() {
        this.closed = true;
    }

    send(data: string) {
        this.onmessage?.(new MessageEvent("message", { data }));
    }
}

describe("the live feed", () => {
    it("keeps parsed entries newest first, skips malformed ones, and closes with its scope", () => {
        const events = new FakeEventSource();
        const scope = effectScope();
        const feed = scope.run(() => useFeed(() => events as unknown as EventSource))!;

        const entry: FeedEntry = {
            kind: "Conflict",
            at: "2026-09-23T12:00:00Z",
            source: "eu",
            table: "public.profiles",
            operation: "update",
            key: '{"id":"7"}',
            commitLsn: "0/3000000",
            sink: "replica",
            detail: "kept us",
        };

        events.onopen?.();
        events.send(JSON.stringify({ ...entry, key: '{"id":"6"}' }));
        events.send(JSON.stringify(entry));
        events.send("{not json");
        events.send(JSON.stringify({ kind: "Explosion" }));

        expect(feed.status.value).toBe("open");
        expect(feed.entries.value.map((item) => item.key)).toEqual(['{"id":"7"}', '{"id":"6"}']);
        expect(feed.rejected.value).toBe(2);

        scope.stop();
        expect(events.closed).toBe(true);
    });
});
