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

SQLiteServer is a purely abstract interaface with no data members. It exists so that SQLite can call methods in
BedrockServer without needing to know everything about Bedrock Server. This leaves BedrockServer as it's own thing
managing it's own data.


STCPNode and SQLiteNode are essentially two halves of one thing. If they're collapsed together, then it's easier to
reason about what they do.

