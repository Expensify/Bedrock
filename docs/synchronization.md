---
layout: default
---

Synchronization
====
Bedrock's primary feature is its ability to seamlessly synchronize data between multiple nodes, manage distributed commits across those nodes, and automatically failover (and recover) in response to failure.  This enables for very reliable configurations [across multiple datacenters](http://bedrockdb.com/multizone.html) (or availability zones).  A general description of how this works follows:

1. Any number of nodes starts up, each of which has a unique priority (except for "permafollowers", which have 0 priority and are invisible from a quorum perspective, but provide additional read capacity).

2. All nodes attempt to connect to all other nodes.

3. During this process all nodes `SYNCHRONIZE` from their peers, which means they broadcast "My most recent transaction has commitCount X, and the hash of every transaction up to that point is Y".  Anybody who has newer data will respond with the missing transactions, which are all committed in the same order on every node via a [private blockchain](http://bedrockdb.com/blockchain.html).

4. Any two nodes that disagree on what the hash of a given transaction should be will immediately disconnect from each other.  This means that any node that has "forked" away from the cluster will be excluded from participation.

5. The Paxos distributed consensus algorithm is used to identify which of the connected nodes has the highest priority.  If enough of the *configured* nodes are online and agree, the highest node will stand up as `LEADER`.  All other nodes begin `FOLLOWING` to that leader.  (On the other hand, if too few of the configured nodes are able to connect so as to achieve quorum, then nobody will stand up, thereby avoiding the "split brain" problem.)

6. Once a node begins `LEADING` or `FOLLOWING`, it opens up its external port to begin accepting traffic from clients (typically webservers).  Clients are typically configured to connect to the "nearest" node from a latency perspective, but all nodes appear equally capable from the outside -- the client has no awareness of who is or isn't the leader.

7. Each node processes read requests from its local database.  By default it will respond based on the latest data.  However, the client can optionally provide a `commitCount`, which if larger than the current commit count of that node's database, will cause the node to hold off on responding until the database has been synchronized up to that point.  In this way, clients can avoid inconsistency by querying two different nodes with different states (though in practice, clients should attempt to query the same node repeatedly to avoid any unnecessary delay).  All of this is provided "out of the box" by Bedrock's [PHP client library](https://github.com/Expensify/Bedrock-PHP).

8. Write commands are escalated to the leader, which coordinates a distributed two-phase commit transaction.  By default, the leader waits for a quorum of followers to approve the transaction, before committing it on the leader database and instructing the followers to do the same.

9. However, a "selective synchronization" algorithm is used to achieve higher write throughput than could be obtained with full quorum alone.  (It requires `median(rtt)` seconds to obtain quorum, limiting total throughput to `1/median(rtt)` full quorum write transactions.)  In this way clients can designate the [level of consistency desired](https://github.com/Expensify/Bedrock/blob/main/sqlitecluster/SQLiteNode.cpp#L1075) on an individual transaction basis, including `QUORUM` (a majority of followers must approve), `ONE` (any follower, typically the nearest), or `ASYNC` (no followers).

10. Obviously, `ASYNC` provides the highest write throughput because the leader commits without waiting.  However, this allows the leader to "race ahead" of the cluster, which is dangerous: if the leader crashes at that point, its unsynchronized commits could be lost forever.  Accordingly, this is recommended only for commits that can be safely lost (eg, a comment on a report) versus a commit that is very dangerous to lose (eg, reimbursing an expense report).

11. Furthermore, for safety, the leader is limited to a maximum number of commits it will go without full quorum, configurable via the `-quorumCheckpoint` command line option.

12. After a write transaction is processed, the response is returned to the node that escalated it, and then back to the client.

13. If the leader dies before an escalated command has been processed, the follower will re-escalate the command to the new leader once elected.  Furthermore, followers will continue accepting commands during the period of leader failover, thereby ensuring that the client sees no "downtime" and merely a short delay (typically imperceptible).

14. When the leader returns to operation, the leader will synchronize any transactions it missed while down, and then stand back up and take over control from the interim leader seamlessly.
