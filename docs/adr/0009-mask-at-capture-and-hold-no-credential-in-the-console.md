# 9. Mask at capture, keep credentials out of the store, and keep the console read-only

Status: accepted

## Context

Change events carry whole rows, and rows carry personal data. Whatever reaches the outbox reaches every sink, every
replay and every backup of the store. The service also holds credentials for sink databases and a signing key for
webhooks, and serves a console.

## Decision

- **Masks apply at capture**, before the outbox, configured per table and column: `Null`, `Redact` (a fixed marker,
  so a masked value is distinguishable from a missing one) or `Hash` (HMAC-SHA-256 with a configured key, so equal
  values stay equal and can still be joined on, and nobody without the key can reverse them by hashing guesses).
  A primary key column can only be hashed; nulling or redacting it would collapse every row into one. Snapshots
  go through the same masking.
- **Sink definitions hold connection names, never connection strings.** Credentials live in configuration or a secret
  store. Anyone with the read token can list sinks, and a webhook URL is shown without its query string in case
  someone put a token there.
- **Webhooks go only to allowed hosts**, and redirects are not followed. Otherwise anyone with the operator token could
  make the service send requests to anything it can reach, cloud metadata endpoints included. Every request is signed
  with a timestamp so a receiver can reject forgeries and replays.
- **Two tokens**, compared in constant time over their SHA-256 hashes: a read token for status and the live feed, and
  an operator token for registering, replaying, snapshotting, retrying and deleting.
- **The console holds no credential.** nginx adds the read token and forwards only the read endpoints the console
  uses, with GET only. Nothing that changes a sink is reachable through it, whatever the browser sends.
- **The capture role is least privilege**: `REPLICATION`, `SELECT` on the published tables, and write access to its
  heartbeat row. The publication is created by the database owner, not by Walrus.

## Consequences

- The masking integration test checks that neither the raw email nor the raw phone number appears anywhere in the
  outbox rows, and that the hashed emails of two rows with the same address are equal.
- Rotating the masking key changes every hashed value from then on, so joins across the rotation break. Rotation is a
  planned event; the operations guide covers it.
- The console cannot retry a dead letter. That is deliberate: an operator with the token does it through the API.

## Alternatives considered

**Mask in each sink.** The outbox, replays and backups would still hold the raw values.

**Encrypt the outbox instead.** Protects the store at rest, but every sink still receives the raw values, which is
usually what masking is for.
