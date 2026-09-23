# 4. Conflicts resolve by hybrid clock, or column by column

Status: accepted

## Context

Two source regions can write the same logical row. Every sink has to end up with the same row, whatever order it
received the two regions' changes in, or the sinks disagree with each other and nobody knows which is right.

LSNs from two databases are unrelated numbers. Commit timestamps are comparable across machines within their
clock skew, but not even monotonic within one database: the timestamp is taken before the commit record is
written, so two concurrent transactions can commit in one order and carry timestamps in the other.

## Decision

Capture stamps each transaction with a hybrid logical clock: milliseconds from the commit timestamp in the upper 48
bits, a counter in the lower 16. It never runs backwards within a source, which absorbs the non-monotonic
timestamps, and across sources it orders by wall clock. Ties between sources go to the source name, which is
arbitrary and deterministic, and that is all a tie-break has to be.

Two policies, chosen per table in configuration:

- **Last writer wins.** The row with the newest version replaces the row.
- **Merge columns.** Each column takes its newest write. Capture works out which columns an update actually wrote
  by comparing the before and after images, which needs `REPLICA IDENTITY FULL`. Two regions changing different
  columns of one row both keep their change. The compose demo does this with a profile: one region moves the
  city, the other changes the plan, and the replica holds both.

A change that loses to a newer change from another source is counted and shown in the console's conflict feed,
with the version that won.

## Consequences

- The conflict integration test writes 1,000 transactions to 20 shared rows from both regions at once into a
  Postgres sink and an index sink, and compares them row by row. They agree, and the Postgres sink reports
  conflicts, so the test is not passing because nothing conflicted.
- Clock skew between regions decides close races. Within the skew, "last writer" means "last by a slightly wrong
  clock". That is the usual price of last-writer-wins, and merge tables shrink what it can cost.
- A merge table without `REPLICA IDENTITY FULL` has no before image, so every column counts as written and it
  behaves as last-writer-wins. The operations guide says so.

## Alternatives considered

**Vector clocks.** They detect concurrency rather than resolving it, and then something still has to choose.
Worth it when the choice needs application knowledge; here the choice is a policy.

**Region priority.** Deterministic and simple, and wrong whenever the lower-priority region made the later change.
