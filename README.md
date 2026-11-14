# Expensify/Bedrock/

This public repo contains the Bedrock database server.  For more
information on what that is, please see https://bedrockdb.com.  The
directories in this repo include:

* `/` - Contains the main Bedrock source
* `/docs` - Source for the public website (hosted via GitHub Pages): https://bedrockdb.com
* `/libstuff` - A general purpose C++ framework for cross-platform application development
* `/mbedtls` - The mbed TLS from here: https://tls.mbed.org/
* `/plugins` - The various plugins provided "out of the box" to Bedrock
* `/sqlitecluster` - The distributed transaction framework built atop sqlite: http://sqlite.org

## C++ editor diagnostics

Ordinary Make targets refresh the ignored `compile_commands.json` in the repository root,
including on incremental and no-op builds. The database includes the server, libraries,
tests, and benchmarks, using the Makefile's compiler commands and flags. Generation requires
Python 3 and runs in the build environment so paths and compiler settings match that environment.

To refresh only the database, without compiling or setting up dependencies, run:

```sh
make compile_commands
```

Use the same compiler and flag overrides as for your build. Generated headers still require
the normal dependency build. Metadata generation preserves the existing database on failure
and does not rewrite an unchanged database. `make clean` skips metadata generation.

