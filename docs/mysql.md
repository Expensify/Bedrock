---
layout: default
---

# Bedrock::MySQL -- MySQL protocol support for Bedrock
Pretty much everybody is familiar with MySQL because it's the "M" in "LAMP" stack.  This means everybody is familiar with how to use the MySQL command line tools, as well as its various language bindings.  However, sometimes despite its familiarity, you [don't want to use MySQL](http://www.bedrockdb.com/vs_mysql.html).  For this reason, Bedrock "speaks MySQL", and listens on the same port that MySQL normally listens on.  Accordingly, you can pretty easily "drop in" Bedrock to replace MySQL with minimal code changes.

## How to connect to Bedrock via the MySQL protocol
However you normally connect to MySQL will work for Bedrock as well.  For example:

    # Install the standard MySQL client library
    $ sudo apt-get install mysql-client
    
    # Run the MySQL command line tool, pointing to localhost
    $ mysql -h 127.0.0.1
    Welcome to the MySQL monitor.  Commands end with ; or \g.
    Your MySQL connection id is 1
    Server version: bedrock f93b67976a61e96ca5671789cdba857e11ec6cc2
    
    Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
    
    Oracle is a registered trademark of Oracle Corporation and/or its
    affiliates. Other names may be trademarks of their respective
    owners.
    
    Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.
    
    mysql> select "Hello Bedrock!";
    +------------------+
    | "Hello Bedrock!" |
    +------------------+
    |   Hello Bedrock! |
    +------------------+
    1 row in set (0.00 sec)
    
    mysql>

Additionally, your standard MySQL language bindings should also "just work".

## How to migrate your existing MySQL service to Bedrock
Migrating to Bedrock is easy:

1. Stop MySQL
2. Dump your database to a big SQL file
3. Read that SQL file using the `.import` command of the [SQLite command line tool](https://www.sqlite.org/cli.html)
4. Move that database to `/var/lib/bedrock/bedrock/db`
5. Start Bedrock
6. Verify your queries are compatible with the [SQLite syntax](https://www.sqlite.org/lang.html)

Bedrock will find and use the existing database, and listen on the MySQL port using the MySQL protocol.  Easy peasy.
