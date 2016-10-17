# Bedrock -- Rock-solid distributed data
Bedrock is a simple, modular, WAN-replicated data foundation for global-scale applications.  Taking each of those in turn:

* **Bedrock is simple.** This means it exposes the fewest knobs necessary, with appropriate defaults at every layer.
* **Bedrock is modular.**  This means its functionality is packaged into separate "plugins" that are decoupled and independently maintainable.
* **Bedrock is WAN-replicated.**  This means it is designed to endure the myriad real-world problems that occur across slow, unreliable internet connections.
* **Bedrock is a data foundation.**  This means it is not just a simple database that responds to queries, but rather a platform on which data-processing applications (like databases, job queues, caches, etc) can be built.
* **Bedrock is for global-scale applications.**  This means it is built to be deployed in a geo-redundant fashion spanning many datacenters around the world.

## Download and Install Bedrock::DB

1. Add the Bedrock:DB apt sources list:

    ```
    sudo wget -O /etc/apt/sources.list.d/bedrockdb.list https://apt.bedrockdb.com/ubuntu/dists/trusty/bedrockdb.list
    ```
2. Add the Bedrock::DB repo key:

    ```
    wget -O - https://apt.bedrockdb.com/bedrockdb.gpg | sudo apt-key add -
    ```
3. Run `sudo apt-get update`.

4. Run `sudo apt-get install bedrock`.

## A Simple Example of Bedrock::DB
To start it:

    ./bedrock -clean -db bedrock.db -serverHost localhost:8888 -nodeName bedrock -nodeHost localhost:8889 -plugins status,db

The parameters mean:

    -clean: Create a new database if one doesn't already exist
    -db <filename>: Use a database with the given name
    -serverHost <host:port>: Listen on this host:port for incoming commands from the application
    -nodeName <name>: Name this specfic node in the cluster as indicated
    -nodeHost <host:port>: Listen on this host:port for connections from other nodes
    -plugins <first,second,...,last>: Enable these plugins

Once started, it listens on *serverHost* for commands implemented by the stated plugins.  To see how this works with the Bedrock::DB plugin, you would connect to *serverHost* using any TCP socket client, send a request, and get the response.  An example with netcat is:

    $ nc localhost 8888
    Query: SELECT 1 AS foo, 2 AS bar;

This would execute the "Query" command of the Bedrock::DB plugin and return the following response:

    200 OK
    Content-Length: 16
    
    foo | bar
    1 | 2

The "200 OK" indicates the command was processed successfully.  "foo | bar" is the header row, and "1 | 2" are the results.  This example only shows a stateless query, but this same technique works with any sqlite compatible query -- including creating tables and index, inserting rows, joining tables, or pretty much everything every other SQL database does.

## How is Bedrock::DB like/unlike MySQL?
On the surface, Bedrock (using the Bedrock::DB plugin) is effectively equivalent to MySQL in that both:

* Store ACID-safe relational data
* Accept SQL requests from clients over a network connection
* Replicate data to multiple hosts
* Execute simultaneous SELECTs using multiple CPUs on the host

To be fair, MySQL has a lot of things that Bedrock doesn't:

* Row-level locking, which when used correctly can enable simultaneous INSERTS on multiple threads
* A huge ecosystem of client apps and platform libraries
* An incredibly huge range of knobs to dial and tweak
* A very familiar name

However, Bedrock has its own tricks up its sleeve, including:

* Simple command-line parameters and a sane default configuration
* "Selective synchronization" to achieve linear scalability of distributed transactions even in high-latency environments 
* No need for application-level awareness of which node is the master
* Automatic "failover" to a slave without requiring application reconfiguration or loss of data
* Automatic recovery back to the master
* C++ as its primary stored procedure language
* A plugin system that combines both schema changes and stored procedures into a self-contained, independently-enableable module
* Advanced connection controls (eg, "Wait until there are results before responding")

In short, both do pretty much anything you need to build an online service.  But while MySQL is optimized for single-datacenter operation with light use of stored procedures, Bedrock is designed from the ground up to operate in a multi-datacenter environment with rigorous use of stored procedures -- the new standard for modern application design.

## Bedrock plugins
Additionally, Bedrock::DB is just one plugin to the overall Bedrock platform.  Bedrock itself is less a database, and more a tool that can be used to build a wide variety of data-management applications -- with a database being just one example.  Each "plugin" implements and exposes new externally-visible commands (essentially equivalent to "stored procdures").  However, unlike simple stored procedures, plugins can also include schema changes.  Plugins can be enabled via the "-plugins" command like parameter.  Current plugins include:

### Bedrock::Status
Provides basic status about the health the database cluster.  Commands include:

 * *Ping()* - Just responds OK
 * *Status()* - Responds with detailed information on the status of all peers (so far as this node knows)

### Bedrock::DB
Provides direct SQL access to the underlying database.  Commands include:

 * *Query( query, [format: json|text] )* - Returns the result of a read query, or executes a write query

For example, this can be used just like any other database.  First, create a table:

    $ nc localhost 8888
    Query: create table foobar ( foo int, bar int );
    
    200 OK

Next insert some data into that table:

    Query: insert into foobar values ( 1, 2 );

    200 OK

Then query data back out of that table:

    Query
    query: select * from foobar;
    format: json

    200 OK
    Content-Length: 40
    
    {"headers":["foo","bar"],"rows":[[1,2]]}
    
### Bedrock::Jobs
(See [Bedrock::Jobs](plugins/Jobs.md))

### Bedrock::Cache
(See [Bedrock::Cache](plugins/Cache.md))


