---
layout: default
---

# Bedrock::Cache -- Simple, distributed, replicated cache
Bedrock::Cache is a plugin to the [Bedrock data foundation](../README.md) providing a cache that is:

* Distributed - Deployable across many nodes for instant local reads
* Replicated - A write on any node replicated to all others.

Commands include:

 * **ReadCache( name )** - Looks up the cached value corresponding to a name, if any
   * *name* - name pattern with which to search the cache (in GLOB syntax)
   * Returns:
     * *name* - name of the value matched (as a header)
     * *value* - raw value associated with that name (in the body of the response) 

 * **WriteCache( name, value )** - Records a named value into the cache, overwriting any other value with the same name.  Also, optionally invalidates other cache entries matching a pattern.
   * *name* - an arbitrary string identifier (case insensitive)
   * *value* - raw data to associate with this value, as a request header (1MB max) or content body (64MB max)
   * *invalidateName* - name pattern to erase from the cache (optional)

## Sample Session
This session shows setting and overriding a simple name/value pair.  First, we just set a value "bar" for the cached named "foo":

    WriteCache
    name: foo
    value: bar

    200 OK
    Content-Length: 0

This can be queried just as you'd expect.  Note that the value is returned in the body of the response:

    ReadCache
    name: foo

    200 OK
    name: foo
    Content-Length: 3

    bar

When writing to the query, you can also invalidate a pattern of existing values.  In this case, we're recording a "v2" of the value, while invalidating all other versions.  Additionally, you can provide the value itself in the body of the request (which means there is no need to base64 encode binary values, or otherwise encode them prior to storage in the cache):

    WriteCache
    name: foo/v2
    invalidateName: foo*
    content-length: 5

    barv2

    200 OK
    Content-Length: 0

Similarly, you can query the cache using a pattern.  In this case, rather than querying a specific version of the value, we're just querying any version available (which due to us invalidating all other versions on write, means this queries the latest version):

    ReadCache
    name: foo*

    200 OK
    name: foo/v2
    Content-Length: 5

    barv2

Like all Bedrock plugins, Bedrock::Cache stores its data in a replicated SQL database.  This means you can query the cache directly using standard SQL (using the Bedrock::DB plugin):

    Query: SELECT * FROM cache;

    200 OK
    Content-Length: 28
    
    name | value
    foo/v2 | barv2
    
Finally, Bedrock::Cache works particularly well with the standard Bedrock `Connection: forget` header.  This feature allows you to queue a command to Bedrock that is executed asynchronously, without "blocking" on its completion.  In this way, you can queue a write to the cache (which will be replicated out to all nodes in the cache) while continuing ahead without delay:

    WriteCache
    name: foo/v3
    invalidateName: foo*
    value: barv3
    connection: forget

    202 Successfully queued
    Content-Length: 0

    ReadCache
    name: foo*

    200 OK
    name: foo/v3
    Content-Length: 4

    barv3

## Using the cache in PHP
If you're using PHP, we've handily provided a client interface in the [Bedrock-PHP](https://github.com/Expensify/Bedrock-PHP/blob/master/src/Cache.php) library.  It works pretty much as you'd expect:

    // Connect to Bedrock
    $bedrock = new Bedrock();
    
    // Write a new version of this cached object (invalidating any previous)
    $bedrock->cache->write("foo", "v4", "value");
    
    // Request the latest version of this object
    $value = $bedrock->read("foo");
    
    // Request a specific version of this object (if available)
    $value = $bedrock->read("foo","v3"); // will return null because v4 invalidated v3
