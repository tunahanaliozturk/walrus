import { z } from "zod";

import type { components } from "./schema";

// Every response is parsed, not cast. Each schema is checked against the type generated from the server's
// OpenAPI document, so a field renamed on the server fails `npm run type-check` here instead of rendering
// undefined in the browser.

type Schemas = components["schemas"];

const count = z.number().int().nonnegative();

export const ColumnValue = z.object({
    name: z.string(),
    value: z.string().nullable(),
}) satisfies z.ZodType<Schemas["ColumnValue"]>;

export const Standby = z.object({
    host: z.string(),
    synced: z.boolean(),
    confirmedFlushLsn: z.string().nullable(),
}) satisfies z.ZodType<Schemas["StandbyResponse"]>;

export const Source = z.object({
    name: z.string(),
    attachedTo: z.string().nullable(),
    primary: z.string().nullable(),
    walRetainedBytes: z.number().nullable(),
    confirmedFlushLsn: z.string().nullable(),
    slotActive: z.boolean(),
    standbys: z.array(Standby),
    changes: count,
    transactions: count,
    resent: count,
    lastCommit: z.string().nullable(),
    lastError: z.string().nullable(),
    checkedAt: z.string().nullable(),
}) satisfies z.ZodType<Schemas["SourceResponse"]>;

export const SinkSource = z.object({
    source: z.string(),
    appliedSeq: count,
    headSeq: count,
    startLsn: z.string(),
    pendingLsn: z.string().nullable(),
    lagMilliseconds: z.number().nonnegative(),
}) satisfies z.ZodType<Schemas["SinkSourceResponse"]>;

export const Sink = z.object({
    id: z.string(),
    kind: z.enum(["Postgres", "Index", "Webhook"]),
    tables: z.array(z.string()),
    target: z.string().nullable(),
    state: z.enum(["Running", "Retrying", "Rebuilding", "Stopped"]),
    blockedRows: count,
    sources: z.array(SinkSource),
    counters: z.object({
        changed: count,
        unchanged: count,
        conflicts: count,
        deadLetters: count,
        retries: count,
        lastError: z.string().nullable(),
    }),
}) satisfies z.ZodType<Schemas["SinkResponse"]>;

export const Stats = z.object({
    at: z.string(),
    sources: z.array(Source),
    sinks: z.array(Sink),
}) satisfies z.ZodType<Schemas["StatsResponse"]>;

export const FeedEntry = z.object({
    kind: z.enum(["Change", "Conflict", "DeadLetter"]),
    at: z.string(),
    source: z.string(),
    table: z.string(),
    operation: z.string(),
    key: z.string(),
    commitLsn: z.string(),
    sink: z.string().nullable(),
    detail: z.string().nullable(),
}) satisfies z.ZodType<Schemas["FeedEntry"]>;

export const FeedEntries = z.array(FeedEntry);

export const DeadLetter = z.object({
    id: count,
    source: z.string(),
    seq: count,
    table: z.string(),
    operation: z.enum(["Insert", "Update", "Delete"]),
    key: z.array(ColumnValue),
    error: z.string(),
    attempts: count,
    deadAt: z.string(),
}) satisfies z.ZodType<Schemas["DeadLetterResponse"]>;

export const DeadLetters = z.array(DeadLetter);

export const SearchResult = z.object({
    rows: z.array(
        z.object({
            table: z.string(),
            key: z.array(ColumnValue),
            row: z.array(ColumnValue),
        }),
    ),
}) satisfies z.ZodType<Schemas["SearchResponse"]>;

export type Stats = z.infer<typeof Stats>;
export type Source = z.infer<typeof Source>;
export type Sink = z.infer<typeof Sink>;
export type FeedEntry = z.infer<typeof FeedEntry>;
export type DeadLetter = z.infer<typeof DeadLetter>;
export type SearchResult = z.infer<typeof SearchResult>;
export type ColumnValue = z.infer<typeof ColumnValue>;
