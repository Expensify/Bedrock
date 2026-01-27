---
layout: default
---
# How does Bedrock compare to MySQL
On the surface, Bedrock (using the [Bedrock::DB](https://bedrockdb.com/db.html) plugin) is effectively equivalent to MySQL in that both:

* Store ACID-safe relational data
* Accept SQL requests from clients over a network connection
* Replicate data to multiple hosts
* Execute simultaneous SELECTs using multiple CPUs on the host
* Compatible with MySQL command-line tools and client libraries

To be fair, MySQL has a lot of things that Bedrock doesn't:

* Row-level locking (Bedrock uses page locking, which is nearly as good in practice)
* A huge ecosystem of client apps and platform libraries (all of which are compatible with Bedrock)
* An incredibly huge range of knobs to dial and tweak
* A very familiar name

However, Bedrock has its own tricks up its sleeve, including:

* Simple command-line parameters and a sane default configuration
* ["Selective synchronization"](https://bedrockdb.com/synchronization.html) to achieve linear scalability of distributed transactions even in high-latency environments 
* No need for application-level awareness of which node is the leader
* Automatic "failover" to a follower without requiring application reconfiguration or loss of data
* Automatic recovery back to the leader
* C++ as its primary stored procedure language
* A plugin system that combines both schema changes and stored procedures into a self-contained, independently-enableable module
* Advanced connection controls (eg, "Wait until there are results before responding")

In short, both do pretty much anything you need to build an online service.  But while MySQL is optimized for single-datacenter operation with light use of stored procedures, Bedrock is designed from the ground up to operate in a multi-datacenter environment with rigorous use of stored procedures -- the new standard for modern application design.
