---
layout: default
---

# Transaction Journal
Bedrock's [synchronization engine](https://bedrockdb.com/synchronization.html) uses a distributed journal. Nodes replicate committed transactions and apply them contiguously, in order.

## The Journal
* Journal entries have three columns:
    * `id` - Simple monotonic index
    * `query` - The query to commit to the database
    * `hash` - A GUID and SHA1 digest identifying and validating the transaction

Every distributed commit adds a journal row. Nodes advertise `CommitCount`, the highest committed ID including blank rows, along with `HashCommitID` and `Hash`, the ID and hash of their latest nonblank entry. If there is no nonblank entry, `HashCommitID` is zero and `Hash` is empty.

To check agreement, the node with more journal entries finds its latest nonblank entry at or before the other node's `CommitCount`, then compares that entry's ID and hash with the advertised `HashCommitID` and `Hash`. Matching identities establish agreement on the shared prefix; disagreement identifies a fork. Trailing blanks do not change agreement, but the node with the larger `CommitCount` is ahead. Synchronization transfers every missing row, including blanks.

## GUID Transaction Hashes
Local transactions always generate `GUID:SHA1`. The GUID is 16 random bytes encoded as 32 hex characters without hyphens. The digest is an uppercase, 40-character hex SHA1 of the GUID text followed by the current transaction's uncompressed SQL, without a colon or previous hash.

This format checks only the current transaction's integrity. The GUID distinguishes independently created transactions even when their SQL is identical. Fork detection compares the complete stored hash and relies on transactions being applied contiguously, in order; it does not verify a cryptographic chain across GUID entries.

`SQLite::prepare()` generates a GUID unless replication supplies the received GUID or an explicit empty string for a blank entry. Both live replication and synchronization recompute and validate nonblank GUID hashes. Chained-hash replay is not supported, so a node must have a GUID-only synchronization tail before upgrading.

## Blank Entries
A blank entry has both an empty query and an empty hash. It consumes an ID without modifying application data. Followers skip SQL execution and commit only the journal row. An empty query with a GUID hash is still a nonblank transaction; an empty hash with a nonempty query is rejected.

Blank entries support future HC-Tree commits that allocate an ID before failing. Local commits do not generate them yet. Deploy blank-aware readers everywhere before enabling blank writers. During this rollout, peers without a `HashCommitID` header are treated as advertising `HashCommitID = CommitCount`.

## Technical Notes
The above skips over a couple important details:

* There are actually multiple `journal` tables, one for each thread (which by default, is equal to the number of cores on the machine).  This is because Bedrock does multi-threaded writes, and given that every commit adds a row to the end of this table, it is very prone to write conflicts.  We address this by "sharding" the table, and then querying them all in a `UNION` whenever we need to view it as one.

* Bedrock exposes the read-only `journalEntries` view for querying all physical journal tables together. For example, use `SELECT * FROM journalEntries WHERE id = 123;` instead of manually building a `UNION ALL` across `journal`, `journal0000`, `journal0001`, and the remaining shards.

* We retain a limited journal history and trim older entries. Trimming always preserves the latest nonblank entry and all rows after it, so a run of blank entries cannot erase the agreement identity needed for synchronization or restart.
