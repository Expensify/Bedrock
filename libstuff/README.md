# libstuff -- All the stuff you need to build a C++ application
C++ is a great language, but the standard libraries suck.  Too many of them devolve into such levels of esoteric abstraction that they become languages onto themselves -- languages that are hard to use to accomplish even the most simple of tasks.  Accordingly, `libstuff` is a very basic library that does all the general things you need to write a basic C++ application, but with more "C" and less "++".  Please see [`libstuff.h`](libstuff.h) for a complete list of commands provided.

## A Simple Example of libstuff
The general idea is that it should handle the mundane details so you can focus on application code.  The basic outline of a libstuff app is:

```
#include <libstuff.h>
int main( int argc, char* argv[] )
{
    // Start libstuff
    SInitialize( );
    SLogLevel( LOG_INFO );

    // Process the command line
    SData args = SParseCommandLine( argc, argv );
    // ... process the arguments ...

    // Keep going until someone kills it (either via TERM or Control^C)
    while (!(SCatchSignal(SIGTERM) || SCatchSignal(SIGINT)))
    {
        // ... do your application ...
    }

    // All done
    SINFO( "Graceful process shutdown complete" );
    return 0;
}
```

## Core Concepts
Computers are super fast these days, and real world applications spend 99% of their CPU time doing some very specific item, with the rest of the time spent doing just boring stuff.  Accordingly, libstuff is not intended to be hyper-efficient: it's intended to be hyper-usable.  This means libstuff uses a lot of `std::string` objects for pretty much everything.  Where a more complex object is required, some higher-level string-based object is used.

For example, need to keep a bunch of name/value pairs together in a single object?  Use `STable` -- a case-insensitive series of named attributes.  Have some kind of command with attributes and maybe some binary content body?  `SData` is for you: a very basic HTTP-like object that can serialize/deserialize a variety of ways.

If you do find some place that needs to be faster, then by all means optimize that specific part.  But don't get hung up on optimizing the parts that don't matter -- which generally covers about 99% of your code.

## Additional Libraries
Included within libstuff are a couple truly amazing open source libraries.  You don't need to worry about the details as libstuff aims to encapsulate them behind a simple, consistent interface.  But they include:

* [SQLite](http://sqlite.org/) - The most widely used and truly incredible database engine on the planet
* [mbed TLS](https://tls.mbed.org/) - A super slick encryption and SSL library

These are packaged inside of libstuff so you needn't deal with downloading and compling them yourself.
