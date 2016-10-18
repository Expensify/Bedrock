---
layout: default
---

Deploying Bedrock in Multiple Zones
====
Bedrock works as well as any other database on a single server, but really shines when deployed across mulitple servers -- ideally in different datacenters (or availability zones) on opposite sides of the internet.  Like all things Bedrock, doing this is much simipler than you'd expect.

Testing multiple Bedrock nodes on a single server
----
Odds are you want to test this out before setting it up in the real world.  To do that:

1. Run three instances of Bedrock at once.  This will require overriding the defaults, so as to allow all three to co-exist without competing for the same filenames or ports:

    asdfadf

2. asdf
3. asdf
