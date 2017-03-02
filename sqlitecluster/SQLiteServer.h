#pragma once
class SQLiteCommand;

class SQLiteServer : public STCPServer {
  public:

    // Constructor initializes underlying STCPServer.
    SQLiteServer(const string& host) : STCPServer(host) { }

    // An SQLiteNode will call this to pass a newly escalated command to a server for processing.
    virtual void acceptCommand(SQLiteCommand&& command) = 0;

    // TODO: not sure if we need both of these.
    // virtual void acceptRequest(SData reqeust) = 0;
};
