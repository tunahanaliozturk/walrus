# Security

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's security advisories for this repository, not in a public
issue. Include what you found, how to reproduce it, and what an attacker could do with it. You should hear back
within a week.

## What Walrus defends, and how

| Concern | What Walrus does |
|---|---|
| Personal data in change events | Masks configured columns at capture, before the outbox, so masked values never reach a sink, a replay or a backup of the store. Hashing uses HMAC-SHA-256 with a secret key |
| Credentials for sink databases | Held in configuration or a secret store. Sink definitions name a connection and never contain a connection string, so listing sinks reveals no credential |
| Requests to arbitrary hosts | Webhooks go only to hosts on an allow-list, and redirects are not followed |
| Forged or replayed webhook deliveries | Every request carries `Walrus-Signature: t=<unix seconds>,v1=<hmac>`, the HMAC-SHA-256 of `t` + `.` + body |
| Who can read and who can change | A read token for status and the live feed; an operator token for every change. Compared in constant time |
| The console | Holds no credential. nginx adds the read token and forwards only GET requests to the read endpoints |
| Access to the source | Capture's role has `REPLICATION`, `SELECT` on the published tables and write access to one heartbeat row. It cannot change the publication or read other tables |
| A misconfigured failover | Capture refuses to resume when the outbox is ahead of the source, instead of silently skipping transactions |
| Configuration mistakes | The service refuses to start with short keys, short tokens or missing settings |

## What is out of scope

- Transport security between Walrus and its databases. Use `SSL Mode=VerifyFull` in the connection strings; the compose
  file does not, because everything in it runs on one machine.
- Encryption at rest of the store. Use the database's or the disk's.
- The console's own login. It is a read-only view meant to sit behind your own authentication.

The keys, tokens and passwords in `compose.yaml` are public and for local use only.
