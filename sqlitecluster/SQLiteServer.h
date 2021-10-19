#pragma once
class SQLiteCommand;

#include <libstuff/STCPmanager.h>

// This is an abstract class to define the interface for a "server" that a SQLiteNode can communicate with to process
// commands it receives.
class SQLiteServer : public STCPManager {
  public:
    // An SQLiteNode will call this to pass a command to a server for processing. The isNew flag is set if this is the
    // first time this command has been sent to this server, as opposed to being an existing command, such as one that
    // was previously escalated.
    virtual void acceptCommand(unique_ptr<SQLiteCommand>&& command, bool isNew) = 0;

    // An SQLiteNode will call this to cancel a command that a peer has escalated but no longer wants a response to.
    // The command may or may not be canceled, depending on whether it's already been processed.
    virtual void cancelCommand(const string& commandID) = 0;

    // This will return true if there's no outstanding writable activity that we're waiting on. It's called by an
    // SQLiteNode in a STANDINGDOWN state to know that it can switch to searching.
    virtual bool canStandDown() = 0;

    // When a node connects to the cluster, this function will be called on the sync thread.
    virtual void onNodeLogin(SQLiteNode::Peer* peer) = 0;
};
