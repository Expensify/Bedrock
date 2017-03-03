#pragma once
class SQLiteCommand;

class SQLiteServer : public STCPServer {
  public:

    // Constructor initializes underlying STCPServer.
    SQLiteServer(const string& host) : STCPServer(host) { }


    // An SQLiteNode will call this to pass a newly escalated command to a server for processing.
    virtual void acceptCommand(SQLiteCommand&& command) = 0;

    // An SQLiteNode will call this to cancel a command that a peer has escalated but no longer wants a response to.
    // The command may or may not be canceled, depending on whether it's already been processed.
    virtual void cancelCommand(const string& commandID) = 0;

    // TODO: not sure if we need both of these.
    // virtual void acceptRequest(SData reqeust) = 0;
};
