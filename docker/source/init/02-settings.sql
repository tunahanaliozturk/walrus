-- Settings that must hold in every way the server can start, not only through the container's command line.
--
-- wal_level: pg_rewind finishes a crashed primary's recovery by starting it in single-user mode, which reads the
-- configuration files and none of the command-line settings. With the default wal_level it refuses to start at
-- all, because the capture slot is a logical slot.
alter system set wal_level = 'logical';

-- wal_keep_size: to rewind a dead primary, pg_rewind reads its log back to the last checkpoint the two nodes
-- share. The crash recovery it runs first ends with a checkpoint that would recycle exactly those segments, and
-- without them the rewind fails and the node needs a full rebuild. Keeping a gigabyte covers any gap between
-- checkpoints this setup produces. A production setup would archive WAL and use --restore-target-wal instead.
alter system set wal_keep_size = '1GB';

-- Commits wait for the standby, so a commit the application saw succeed is on both nodes and survives the
-- primary dying. The standbys are named, never '*': a logical replication connection is a standby too, and with
-- '*' Walrus's own connection becomes the synchronous one. Every commit then waits for Walrus to store it, and the
-- physical standby, the node that has to survive a failover, is only a potential one. FIRST 1 over both names
-- works on either node: a node is never its own standby, so the other one is always the one that counts.
--
-- Set here rather than on the command line because the first-start server that runs these init scripts uses the
-- command-line settings, and with no standby yet every commit above would wait forever. It takes effect from the
-- next start, which is the real one.
alter system set synchronous_standby_names = 'FIRST 1 (pg_a, pg_b)';
