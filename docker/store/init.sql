-- The store (Walrus's own database, schema created by its migrations) and a sink database to replicate into.
create database walrus;
create database sink;

\connect sink

create table public.accounts (
    id         bigint primary key,
    owner      text          not null,
    email      text,
    balance    numeric(18,2) not null default 0,
    seq        bigint        not null default 0,
    written_at timestamptz   not null default clock_timestamp()
);

create table public.profiles (
    id   bigint primary key,
    name text,
    city text,
    plan text
);

-- Every version of every account the sink applies, with when it was written at the source and when it
-- landed here. The load harness reads its lag and its ordering from this table. Both timestamps come from
-- Postgres containers on one machine, so they share a clock.
create table public.accounts_applied (
    n          bigserial primary key,
    id         bigint      not null,
    seq        bigint      not null,
    written_at timestamptz not null,
    applied_at timestamptz not null default clock_timestamp()
);

create function public.record_account() returns trigger language plpgsql as $$
begin
    insert into public.accounts_applied (id, seq, written_at) values (new.id, new.seq, new.written_at);
    return new;
end $$;

create trigger record_account after insert or update on public.accounts
    for each row execute function public.record_account();
