#pragma once
class SQLiteCommand;

// This is an abstract class to define the interface for a "server" that a SQLiteNode can communicate with to process
// commands it receives.
class SQLiteServer : public STCPServer {
  public:
    // Constructor initializes underlying STCPServer.
    SQLiteServer(const string& host) : STCPServer(host) { }

    // An SQLiteNode will call this to pass a newly escalated command to a server for processing.
    virtual void acceptCommand(SQLiteCommand&& command) = 0;

    // An SQLiteNode will call this to cancel a command that a peer has escalated but no longer wants a response to.
    // The command may or may not be canceled, depending on whether it's already been processed.
    virtual void cancelCommand(const string& commandID) = 0;

    // This will return true if there's no outstanding writable activity that we're waiting on. It's called by an
    // SQLiteNode in a STANDINGDOWN state to know that it can switch to searching.
    virtual bool canStandDown() = 0;

    // When a node connects to the cluster, this function will be called on the sync thread.
    virtual void onNodeLogin(SQLiteNode::Peer* peer) = 0;
};
