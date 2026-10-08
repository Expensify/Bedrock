---
layout: default
---

# Bedrock -- Command line options
To see a full list of Bedrock's configuration options, just run `bedrock -?` on the command line:

	$ bedrock -?
	Usage:
	------
	bedrock [-? | -h | -help]
	bedrock -version
	bedrock [-clean] [-v] [-db <filename>] [-serverHost <host:port>] [-nodeHost <host:port>] [-nodeName <name>] [-peerList <list>] [-priority <value>] [-plugins <list>] [-cacheSize <kb>] [-workerThreads <#>] [-versionOverride <version>]

	Common Commands:
	----------------
	-?, -h, -help               Outputs instructions and exits
	-version                    Outputs version and exits
	-v                          Enables verbose logging
	-clean                      Recreate a new database from scratch
	-versionOverride <version>  Pretends to be a different version when talking to peers
	-db             <filename>  Use a database with the given name (default 'bedrock.db')
	-serverHost     <host:port> Listen on this host:port for cluster connections (default 'localhost:8888')
	-nodeName       <name>      Name this specfic node in the cluster as indicated (defaults to the value of $hostname)
	-nodeHost       <host:port> Listen on this host:port for connections from other nodes
	-peerList       <list>      See below
	-priority       <value>     See '-peerList Details' below (defaults to 100)
	-plugins        <list>      Enable these plugins (defaults to 'status,db,jobs,cache')
	-cacheSize      <kb>        number of KB to allocate for a page cache (defaults to 1GB)
	-workerThreads  <#>         Number of worker threads to start (min 1, defaults to number of CPU cores)
	-queryLog       <filename>  Set the query log filename (default 'queryLog.csv', SIGUSR2/SIGQUIT to enable/disable)
	-maxJournalSize <#commits>  Number of commits to retainin the historical journal (default 1000000)

	Quick Start Tips:
	-----------------
	In a hurry?  Just run 'bedrock -clean' the first time, and it'll create a new database called 'bedrock.db', then use all the defaults listed above.  (After the first time, leave out the '-clean' to reuse the same database.)  Once running, you can verify it's working using NetCat to manualy send a Ping request as follows:

	$ bedrock -clean &
	$ nc local 8888
	Ping

	200 OK

	-peerList Details:
	------------------
	The -peerList parameter enables you to configure multiple Bedrock nodes into a redundant cluster.  Bedrock supports any number of nodes: simply start each node with a comma-separated list of the '-nodeHost' of all other nodes.  You can safely send any command to any node.  Some best practices:

	- Put each Bedrock node on a different server.

	- Assign each node a different priority (greater than 0).  The highest priority node will be the 'leader', which will coordinate distributed transactions.


