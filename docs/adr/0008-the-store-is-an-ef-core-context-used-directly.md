# 8. The store is an EF Core context, used directly, with one COPY on the hot path

Status: accepted

## Context

Walrus's own database holds the outbox, the capture checkpoints, the sinks, their cursors and their dead letters.
The development plan suggested Dapper, on the grounds that the SQL for the outbox and the watermarks is the
correctness argument and should stay visible. The house rule for these projects is an EF Core `DbContext` used
directly by the code that needs it, with no generic repository on top.

## Decision

`WalrusDbContext` owns the schema through migrations, and the adapters use it directly: a pooled context factory,
a short-lived context per unit of work, no repository layer. Reads are LINQ with `AsNoTracking`. Updates that are
one statement use `ExecuteUpdateAsync` and `ExecuteDeleteAsync`. Upserts that need `ON CONFLICT` use
`ExecuteSqlAsync` with interpolated parameters.

Two places leave LINQ, each with the reason at the call site:

- **The outbox write** is a binary `COPY` on the context's own connection, inside the context's own transaction,
  after the epoch-fenced checkpoint update. It is the hot path, and an `INSERT` per change through the change tracker
  would cost more than everything else capture does.
- **The capture lease** is an advisory lock, which belongs to a server session, so it lives on one dedicated
  connection for as long as the session runs.

Sink tables are not in the model at all. They belong to whoever owns the sink database, their columns are only known
at run time, and the Postgres sink writes them with explicit parameterised SQL through Npgsql.

Row images are stored as `json`, not `jsonb`: `jsonb` sorts keys, and a row's column order is part of what a sink
materialises. Nothing queries inside them.

## Consequences

- The correctness argument is still readable: the checkpoint update, the epoch check and the COPY sit together in
  `CaptureStore.PersistAsync`, in order, in one transaction.
- Schema changes are migrations, applied at start by the host before capture or dispatch runs.

## Alternatives considered

**Dapper throughout.** Would have been fine, and would have broken the house rule for no gain the two exceptions do
not already cover.

**A repository per aggregate over the context.** A second layer that forwards calls, and a place for the unit of
work to leak.
