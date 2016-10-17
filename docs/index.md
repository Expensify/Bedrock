---
layout: default
---

# Bedrock -- Rock-solid distributed data
Bedrock is a simple, modular, WAN-replicated data foundation for global-scale applications.  Taking each of those in turn:

* **Bedrock is simple.** This means it exposes the fewest knobs necessary, with appropriate defaults at every layer.
* **Bedrock is modular.**  This means its functionality is packaged into separate "plugins" that are decoupled and independently maintainable.
* **Bedrock is WAN-replicated.**  This means it is designed to endure the myriad real-world problems that occur across slow, unreliable internet connections.
* **Bedrock is a data foundation.**  This means it is not just a simple database that responds to queries, but rather a platform on which data-processing applications (like databases, job queues, caches, etc) can be built.
* **Bedrock is for global-scale applications.**  This means it is built to be deployed in a geo-redundant fashion spanning many datacenters around the world.

Bedrock was built by [Expensify](https://www.expensify.com), and is a networking and distributed transaction layer built atop [SQLite](http://sqlite.org/), the fastest, most reliable, and most widely distribute database in the world.

## How to get it
Bedrock can be compiled from source using the [Expensify/Bedrock](https://github.com/Expensify/Bedrock) public repo, or installed into your Ubuntu environment using the following commands:

    # Add the Bedrock repo to apt sources:
    sudo wget -O /etc/apt/sources.list.d/bedrock.list https://apt.bedrockdb.com/ubuntu/dists/trusty/bedrock.list

    # Add the Bedrock repo key:
    wget -O - https://apt.bedrockdb.com/bedrock.gpg | sudo apt-key add -

    # Update the apt-get and install Bedrock
    sudo apt-get update
    sudo apt-get install bedrock

## How to use it
Bedrock is so easy to use, you'll think you're missing something.  Once installed, Bedrock listens on `localhost` port 8888, and stores its database in `/var/lib/bedrock`.  The easiest way to talk with Bedrock is using `netcat` as follows:

    $ nc localhost 8888
    Query: SELECT 1 AS foo, 2 AS bar;

That query can be any [SQLite-compatible query](http://sqlite.org/lang.html) -- including schema changes, foreign key constraints, partial indexes, native JSON expressions, or any of the tremendous amount of functionality SQLite offers.  The result will be returned in an HTTP-like response format:

    200 OK
    Content-Length: 16
    
    foo | bar
    1 | 2

By default, Bedrock optimizes the output for human consumption.  If you are a robot, request JSON output:

    $ nc localhost 8888
    Query
    query: SELECT 1 AS foo, 2 AS bar;
    format: json

    200 OK
    Content-Length: 16

    {...}

Some people are creeped out by sockets, and prefer tools.  No problem: Bedrock supports the MySQL protocol, meaning you can continue using whatever MySQL client you prefer:

    $ mysql
    ...

That also means you can continue using whatever MySQL language binding you already know and love.  Alternatively, if you don't like any of them, Bedrock also provides a [PHP binding](https://github.com/Expensify/Bedrock-PHP) that looks something like this:

    $bedrock = new Bedrock();
    $result = $bedrock->db->query("SELECT 1 AS foo, 2 AS bar;");

It really can be that easy.

## Bedrock plugins
Additionally, Bedrock::DB is just one plugin to the overall Bedrock platform.  Bedrock itself is less a database, and more a tool that can be used to build a wide variety of data-management applications -- with a database being just one example.  Each "plugin" implements and exposes new externally-visible commands (essentially equivalent to "stored procdures").  However, unlike simple stored procedures, plugins can also include schema changes.  Plugins can be enabled via the "-plugins" command like parameter.  Current plugins include:

* [Status](status.html) - Provides basic status about the health the Bedrock cluster.
* [DB](db.html) - Provides direct SQL access to the underlying database.
* [Jobs](jobs.html) - Provides a simple job queue.
* [Cache](cache.html) - Provides a simiple replicated cache.

## How to get help
So many ways!

* Chat with us live on [Bedrock's Gitter page](https://gitter.im/Expensify-Bedrock/Lobby)
* Post to the [Bedrock mailing list](https://groups.google.com/forum/#!forum/bedrock) by emailing [bedrock@googlegroups.com](mailto:bedrock@googlegroups.com)
* Create an issue in [Bedrock's GitHub issue list](https://github.com/Expensify/Bedrock/issues)
* Email David, the CEO of Expensify (and biggest Bedrock fanboy ever) directly: [dbarrett@expensify.com](mailto:dbarrett@expensify.com)


