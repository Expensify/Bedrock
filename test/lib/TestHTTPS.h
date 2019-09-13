#pragma once
#include <libstuff/libstuff.h>

class TestHTTPS : public SHTTPSManager {
  public:
    TestHTTPS(const atomic<SQLiteNode::State>& replicationState) : SHTTPSManager(replicationState) { }
    virtual ~TestHTTPS();

    // SHTTPSManager API
    virtual bool _onRecv(Transaction* transaction);
    virtual Transaction* sendRequest(const string& url, SData& request);
};
