const whole = new Intl.NumberFormat("en-GB", { maximumFractionDigits: 0 });
const one = new Intl.NumberFormat("en-GB", { maximumFractionDigits: 1 });

/** A count with thousands separators. */
export function count(value: number): string {
    return whole.format(value);
}

/** A rate per second, whole above ten and with one decimal below. */
export function rate(perSecond: number): string {
    return `${perSecond >= 10 ? whole.format(perSecond) : one.format(perSecond)}/s`;
}

/** A duration in milliseconds, switching to seconds from one second up. */
export function duration(milliseconds: number): string {
    if (milliseconds < 1_000) {
        return `${whole.format(milliseconds)} ms`;
    }

    return milliseconds < 60_000
        ? `${one.format(milliseconds / 1_000)} s`
        : `${one.format(milliseconds / 60_000)} min`;
}

/** Bytes in the largest binary unit that keeps the number at or above one. */
export function bytes(value: number | null): string {
    if (value === null) {
        return "unknown";
    }

    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let size = value;
    let unit = 0;

    while (size >= 1024 && unit < units.length - 1) {
        size /= 1024;
        unit++;
    }

    return `${unit === 0 ? whole.format(size) : one.format(size)} ${units[unit]}`;
}

/** How long ago an instant was, coarsely, relative to now. */
export function ago(instant: string | null, now: number = Date.now()): string {
    if (instant === null) {
        return "never";
    }

    const seconds = Math.max(0, Math.round((now - Date.parse(instant)) / 1_000));

    if (seconds < 60) {
        return `${seconds} s ago`;
    }

    return seconds < 3_600
        ? `${Math.round(seconds / 60)} min ago`
        : `${Math.round(seconds / 3_600)} h ago`;
}

/** A primary key as the feed sends it, JSON, turned into id=7, region=eu. */
export function key(json: string): string {
    try {
        const parsed: unknown = JSON.parse(json);

        if (typeof parsed === "object" && parsed !== null) {
            return Object.entries(parsed)
                .map(([name, value]) => `${name}=${String(value)}`)
                .join(", ");
        }
    } catch {
        // Fall through and show it as it came.
    }

    return json;
}
