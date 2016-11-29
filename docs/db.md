---
layout: default
---

# Bedrock::DB
Provides direct SQL access to the underlying database.  Commands include:

 * *Query( query, [format: json&#124;text] )* - Returns the result of a read query, or executes a write query

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
