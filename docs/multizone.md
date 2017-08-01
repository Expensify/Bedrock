---
layout: default
---

Deploying Bedrock in Multiple Zones
====
Bedrock works as well as any other database on a single server, but really shines when deployed across mulitple servers -- ideally in different datacenters (or availability zones) on opposite sides of the internet.  Like all things Bedrock, doing this is much simipler than you'd expect.

Testing multiple Bedrock nodes on a single server
----
Odds are you want to test this out before setting it up in the real world by running three nodes on one server.  This works fine, but requires some trickery.  First, add three entries to your `/etc/hosts` file to simulate multiple servers -- all of which point to `localhost`:

    127.0.0.1 node0
    127.0.0.1 node1
    127.0.0.1 node2

Now you can run three instances of Bedrock at once.  This will require overriding the defaults, so as to allow all three to co-exist without competing for the same filenames or ports:

    sudo bedrock -clean -nodeName node0 -db node0.db -priority 100 -serverHost localhost:8000 -nodeHost localhost:9000 -peerList node1:9001,node2:9002 -fork
    sudo bedrock -clean -nodeName node1 -db node1.db -priority 101 -serverHost localhost:8001 -nodeHost localhost:9001 -peerList node0:9000,node2:9002 -fork
    sudo bedrock -clean -nodeName node2 -db node2.db -priority 102 -serverHost localhost:8002 -nodeHost localhost:9002 -peerList node0:9000,node1:9001 -fork

Verify all three are running by running `pgrep bedrock` and confirming three PIDs are returned.  And really, that's it.  You've got three servers executing in a perfectly synchronized fashion.  You can send any command to any server, and it'll work fine.  For fun, try the following:

* Kill any server, and you'll notice the other two work fine.
* Start the server back up and you'll notice it re-synchronizes down the data it missed while down.
* Kill two servers, and the remaining will stop working because it realizes it doesn't have "quorum" and thus it's safer to do nothing than risk a "split-brain" scenario where multiple nodes *think* they are the only one alive, only to later realize that there were multiple alive who merely couldn't talk temporarily.

Deploying multiple Bedrock nodes across multiple servers
----
The Bedrock portion of running a multi-datacenter environment is actually remarkably simple: run the same commands as above, except rather than having `/etc/hosts` resolve the node names to `localhost`, resolve them to the remote server IP address.  It's really that easy, so far as Bedrock is concerned.

The difficulty is in doing that in a secure fashion.  The recommended way to do this is to set up a point-to-point VPN (such as using OpenVPN) between the datacenters, such that all servers across all datacenters appear to be on a single unified LAN.  Doing this is unfortunately out of the scope of this document.

If you just want to screw around and do some more testing, however you might just open up the external ports directly and then use IP filtering to lock down traffic to only that which comes from the other servers.  This isn't nearly secure enough for real production use, but should allow you to continue testing Bedrock.

Configuring your web application to use the Bedrock cluster
---
The recommended method to deploy a reliable web application is to have at least one webserver for each Bedrock node.  Every webserver should first attempt to connect to the "local" Bedrock node (which will go over the LAN and thus be incredibly fast for all read traffic), but then "fail over" to a load-balanced pool of the "remote" servers.  This way you can safely take down any Bedrock node for maintenance and your application layer instantly redirects to the remote servers.  Obviously, you want to keep all your nodes up for the best performance, but for off-peak maintenance with zero-downtime, it's hard to beat.
