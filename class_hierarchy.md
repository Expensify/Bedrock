Class Hierarchy:

```
STCPManager ═════════════════════════════╗
STCPManager::Socket                      ║
STCPManager::Port                        ║
                                         ║
     ╔═══════════════════════════════════╬══════════════════════════════╗
  STCPNode                         SQLiteServer                SStandaloneHTTPSManager
     ║                                   ║                     SStandaloneHTTPSManager::Transaction
 SQLiteNode                       BedrockServer

```
Notes:

STCPManager has no data members of its own. It merely provides it's nested classes (TODO: un-nest them) and some static
methods useful by the derived classes. Particularly it provides the "main" implementations of prePoll and postPoll that
are used in the various derived classes.

STCPManager::Socket *should be* thread-safe such that a socket can be read, written, or any other operation by multiple
threads correctly. TODO: Go through and re-verify this.

STCPManager::Port contains only const data members, and as such is implicitly thread-safe except in the case of
creation or destruction.

The lack of data members inside STCPManager is intentional. It allows for much easier reasoning about what needs to be
locked, and where and when. If all of the synchronization for a particular class is localized inside that class itself,
and not scattered down a hierarchy of parent classes, it's much easier to reason about and get correct.

SQLiteServer is a purely abstract interface with no data members. It exists so that SQLite can call methods in
BedrockServer without needing to know everything about Bedrock Server. This leaves BedrockServer as it's own thing
managing it's own data.

BedrockServer, then, contains all it's own data and can do it's own reasoning about when it needs to lock any of it's
own data structures or not.

STCPNode and SQLiteNode are essentially two halves of one thing. If they're collapsed together, then it's easier to
reason about what they do. Until then, they collectively can be reasoned about as a unit, such that the locks in
SQLiteNode need to be aware of what's happening in STCPNode. Collapsing these together is an exercise for the future.

