import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

// Runs against the compose stack after CI has registered the sinks and written to both sources. The console
// reads through nginx, which adds the read token; nothing here holds a credential.

test("the overview shows both sources capturing and the sinks caught up", async ({ page }) => {
    await page.goto("/");

    await expect(page.getByRole("heading", { level: 3, name: /eu/ })).toBeVisible();
    await expect(page.getByRole("heading", { level: 3, name: /us/ })).toBeVisible();
    // Whichever node is the standby now: an odd number of failovers earlier in the run leaves it on pg-a.
    await expect(page.getByText(/Ready: slot synchronised to pg-[ab]:5432/)).toBeVisible();

    const sinks = page.getByRole("table");
    await expect(sinks.getByRole("rowheader", { name: "replica" })).toBeVisible();
    await expect(sinks.getByRole("rowheader", { name: "search" })).toBeVisible();

    const results = await new AxeBuilder({ page }).analyze();
    expect(results.violations).toEqual([]);
});

test("the live page streams captured changes as they are written", async ({ page }) => {
    await page.goto("/live");
    await expect(page.getByText("Stream open.")).toBeVisible();

    // Written by the CI step that runs alongside this test, every few hundred milliseconds.
    await expect(
        page.getByRole("list", { name: "Events, newest first" }).getByRole("listitem").first(),
    ).toBeVisible({
        timeout: 20_000,
    });

    const results = await new AxeBuilder({ page }).analyze();
    expect(results.violations).toEqual([]);
});

test("a sink's page shows its position and dead letters", async ({ page }) => {
    await page.goto("/sinks/replica");

    await expect(page.getByRole("heading", { level: 1, name: "Sink replica" })).toBeVisible();
    await expect(page.getByRole("rowheader", { name: "eu" })).toBeVisible();
    await expect(page.getByRole("heading", { level: 2, name: "Dead letters" })).toBeVisible();

    const results = await new AxeBuilder({ page }).analyze();
    expect(results.violations).toEqual([]);
});

test("the index sink finds a row by its words", async ({ page }) => {
    await page.goto("/search");

    await page.getByLabel("Words").fill("linus");
    await page.getByRole("button", { name: "Search" }).click();

    await expect(page.getByRole("cell", { name: "public.profiles" })).toBeVisible();

    const results = await new AxeBuilder({ page }).analyze();
    expect(results.violations).toEqual([]);
});
