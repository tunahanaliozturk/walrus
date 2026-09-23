import type { z } from "zod";

import { DeadLetters, FeedEntries, SearchResult, Stats } from "./contract";

/** A failed request, with the title the server gave it. */
export class ApiError extends Error {
    constructor(
        readonly status: number,
        message: string,
    ) {
        super(message);
        this.name = "ApiError";
    }
}

// No credential anywhere in the browser: the console is served by nginx, which adds the read token to the
// handful of read endpoints it forwards. Nothing that changes a sink is reachable from here.
async function get<T>(path: string, schema: z.ZodType<T>): Promise<T> {
    const response = await fetch(path, { headers: { Accept: "application/json" } });

    if (!response.ok) {
        let title = response.statusText || `HTTP ${response.status}`;

        try {
            const body: unknown = await response.json();

            if (
                typeof body === "object" &&
                body !== null &&
                "title" in body &&
                typeof body.title === "string"
            ) {
                title = body.title;
            }
        } catch {
            // Not JSON: keep the status line.
        }

        throw new ApiError(response.status, title);
    }

    return schema.parse(await response.json());
}

export const api = {
    stats: () => get("/v1/stats", Stats),
    conflicts: () => get("/v1/conflicts", FeedEntries),
    deadLetters: (sink: string) =>
        get(`/v1/sinks/${encodeURIComponent(sink)}/dead-letters`, DeadLetters),
    search: (sink: string, query: string) =>
        get(
            `/v1/sinks/${encodeURIComponent(sink)}/search?q=${encodeURIComponent(query)}&limit=25`,
            SearchResult,
        ),
};
