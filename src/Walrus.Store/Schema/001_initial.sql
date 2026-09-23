-- The durable hand-off point between the source's log and every sink.
--
-- A change is acknowledged to the source's replication slot only after the transaction that inserts it here
-- has committed. Everything else in this service is built on that one ordering.

create table walrus_outbox (
    seq             bigint generated always as identity primary key,
    source          text        not null,
    commit_lsn      pg_lsn      not null,
    ordinal         integer     not null,
    xid             bigint      not null,
    commit_ts       timestamptz not null,
    hlc             bigint      not null,
    table_name      text        not null,
    op              char(1)     not null check (op in ('c', 'u', 'd')),

    -- json rather than jsonb. jsonb sorts object keys, and a row image's column order is part of what a sink
    -- materialises. It is also cheaper to insert, and nothing ever queries inside these columns.
    key             json        not null,
    before          json,
    after           json,
    changed_columns text[],
    captured_at     timestamptz not null default now(),

    -- Reading the same transaction from the log twice, which is what happens after a crash, produces the same
    -- three values. This constraint is what turns that re-read into a no-op instead of a duplicate.
    constraint walrus_outbox_change unique (source, commit_lsn, ordinal)
);

-- Dispatch reads one source at a time in capture order. A single capture writer per source is what makes the
-- sequence gap-free within a source; across sources, two writers commit concurrently and a reader of the whole
-- table could skip a sequence number that was still in flight.
create index walrus_outbox_by_source on walrus_outbox (source, seq);

-- Replay and snapshot seams start from a log position rather than a sequence number.
create index walrus_outbox_by_lsn on walrus_outbox (source, commit_lsn);

create index walrus_outbox_by_capture_time on walrus_outbox (captured_at);

create table walrus_capture_state (
    source        text        primary key,

    -- Incremented by every capture session as it starts. A session commits only while the epoch is still its
    -- own, so a session that lost its replication connection cannot finish persisting a transaction after its
    -- replacement has started, which would let two writers stamp one source's clock out of order.
    epoch         bigint      not null default 0,
    confirmed_lsn pg_lsn      not null default '0/0',
    last_hlc      bigint      not null default 0,
    updated_at    timestamptz not null default now()
);

create table walrus_sinks (
    id         text        primary key check (id ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
    kind       text        not null check (kind in ('postgres', 'index', 'webhook')),
    config     json        not null,
    created_at timestamptz not null default now()
);

create table walrus_sink_cursors (
    sink_id    text        not null references walrus_sinks (id) on delete cascade,
    source     text        not null,
    last_seq   bigint      not null default 0,

    -- Changes committed before this position are not delivered. A snapshot-bootstrapped sink already has them
    -- from the snapshot, and delivering them again would be harmless but slow.
    start_lsn  pg_lsn      not null default '0/0',
    updated_at timestamptz not null default now(),
    primary key (sink_id, source)
);

create table walrus_dead_letters (
    id          bigint      generated always as identity primary key,
    sink_id     text        not null references walrus_sinks (id) on delete cascade,
    source      text        not null,
    seq         bigint      not null,
    entity_key  text        not null,
    event       json        not null,
    error       text        not null,
    attempts    integer     not null,
    dead_at     timestamptz not null default now(),
    resolved_at timestamptz
);

-- The question dispatch asks for every change: does this row already have an unresolved failure, so that this
-- change has to queue behind it rather than overtake it.
create index walrus_dead_letters_open on walrus_dead_letters (sink_id, entity_key) where resolved_at is null;
