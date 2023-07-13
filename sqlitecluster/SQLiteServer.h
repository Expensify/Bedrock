#pragma once
class SQLiteCommand;
class SQLitePeer;

#include <libstuff/STCPManager.h>

// This is an abstract class to define the interface for a "server" that a SQLiteNode can communicate with to process
// commands it receives.
class SQLiteServer : public STCPManager {
  public:
    // This will return true if there's no outstanding writable activity that we're waiting on. It's called by an
    // SQLiteNode in a STANDINGDOWN state to know that it can switch to searching.
    virtual bool canStandDown() = 0;

    // When a node connects to the cluster, this function will be called on the sync thread.
    virtual void onNodeLogin(SQLitePeer* peer) = 0;

    virtual void notifyPlugins(SQLiteNodeState newState) = 0;
};
