#pragma once
class SQLiteCommand;
class SQLitePeer;
#include "SQLiteNode.h"

#include <libstuff/STCPManager.h>

// This is an abstract class to define the interface for a "server" that a SQLiteNode can communicate with to process
// commands it receives.
class SQLiteServer : public STCPManager {
  public:
    // When a node connects to the cluster, this function will be called on the sync thread.
    virtual void onNodeLogin(SQLitePeer* peer) = 0;

    // We call this method whenever a node changes state
    virtual void notifyStateChangeToPlugins(SQLite& db, SQLiteNodeState newState) = 0;

    // You must block and unblock the command port with *identical strings*.
    virtual void blockCommandPort(const string& reason) = 0;
    virtual void unblockCommandPort(const string& reason) = 0;
};
