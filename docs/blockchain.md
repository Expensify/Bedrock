---
layout: default
---

# Blockchain -- Bedrock's secret sauce (before it was cool)
Though the blockchain hype has largely come and gone, it's worth pointing out that the key to Bedrock's [synchronization engine](https://bedrockdb.com/synchronization.html) is a distributed general ledger -- aka, a blockchain.  So it wouldn't be crazy to say that Bedrock is at is core, a blockchain-based database, built in 2007.  (And for those who are keeping score: that's two years before [Bitcoin](https://en.wikipedia.org/wiki/Bitcoin) came onto the scene, and a year before [Satoshi Nakamoto](https://en.wikipedia.org/wiki/Satoshi_Nakamoto) supposedly invented the blockchain.  But hey, who's counting.)

## Public vs Private Blockchains
To be clear, Bedrock is not a "public" blockchain, like Bitcoin -- it is not designed to synchronize a series of records between thousands or millions of anonymous peers over the open internet.  Rather, Bedrock uses a "private" blockchain, meaning that a small cluster of servers (3-6) operating in a controlled environment connect to each other on an equal footing to synchronize a historical record of commits, and apply them in the same order.

## The Journal
Under the hood it works like this:

* There is an internal table named `journal`, that has three columns:
    * `id` - Simple monotonic index
    * `query` - The query to commit to the database
    * `hash` - A SHA1 hash of `query`, combined with the previous `hash`
    
* Every time a query is committed to the database, a new row is inserted into the journal.  This row records the query, and calculates the new incremental hash -- based on the last row in the database

* This means that every row is actually a product of every single row that has come before it.

* So when a server connects to the cluster, it broadcasts its most recent `id` and `hash`, and then any two servers can confirm that they agree on the history up to that point.  And since all queries are committed in the same order, that means that the two servers are confident that they have the same exact database.

* In the event two servers disagree on what hash corresponds to a given `id`, they know that they have "forked" at some point in the past, and simply refuse to communicate any futher.  This means in a "split brain" scenario, those subsets of the cluster that agree will connect to each other, and refuse to connect to the others.  (And then our Paxos-based election scheme will ensure that only one of "splits" will stand up a new leader, because the others do not have quorum.)

* After two nodes have connected and confirm they agree on the history up to a point, then if one has more data than the other, it will download each commit and apply it in turn -- every time confirming that it still agrees with the hash of the peer.

The sum of all this ensures that all of the cluster stays in perfect sync (and refuses to talk to those nodes that have forked), all without any of them being "in charge".  This is the heart of what makes a distributed ledger so special, and has processed (as of this writing) 4,287,514,530 successful commits on the database to date.

## Technical Notes
The above skips over a couple important details:

* There are actually multiple `journal` tables, one for each thread (which by default, is equal to the number of cores on the machine).  This is because Bedrock does multi-threaded writes, and given that every commit adds a row to the end of this table, it is very prone to write conflicts.  We address this by "sharding" the table, and then querying them all in a `UNION` whenever we need to view it as one.

* We don't actually retain all 4B+ rows to the journal.  Rather, we do full backups at least nightly, and instead just keep several days of history in the journal, trimming as we go. 
