-- The source database: the tables an application writes, and what Walrus needs to capture them.

-- Capture's role can stream and read the published tables, and write its heartbeat. Nothing else: it cannot
-- change the publication, read other tables or write application data.
create role walrus_capture with login replication password 'capture-password-for-local-use-only';

-- The standby streams through this role, never as a superuser.
create role replicator with login replication password 'replicator-password-for-local-use-only';

create schema walrus;
create table walrus.heartbeat (source text primary key, beat_at timestamptz not null);
grant usage on schema walrus to walrus_capture;
grant select, insert, update on walrus.heartbeat to walrus_capture;

-- Replica identity full, so updates and deletes carry the row as it was. That costs log volume and buys before
-- images, which merge tables need and which the live feed shows.
create table public.accounts (
    id         bigint primary key,
    owner      text          not null,
    email      text,
    balance    numeric(18,2) not null default 0,
    seq        bigint        not null default 0,
    written_at timestamptz   not null default clock_timestamp()
);
alter table public.accounts replica identity full;

create table public.profiles (
    id   bigint primary key,
    name text,
    city text,
    plan text
);
alter table public.profiles replica identity full;

grant select on public.accounts, public.profiles to walrus_capture;

-- TRUNCATE is left out on purpose: a sink cannot tell a truncate from a mass delete it should replay.
create publication walrus for table public.accounts, public.profiles, walrus.heartbeat
    with (publish = 'insert, update, delete');
