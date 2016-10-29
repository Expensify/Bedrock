#pragma once
#include <libstuff/SHTTPSManager.h>
#include <libstuff/libstuff.h>

class TestHTTPS : public SHTTPSManager {
  public:
    TestHTTPS();
    virtual ~TestHTTPS();

    // SHTTPSManager API
    virtual bool _onRecv(Transaction* transaction);
    virtual Transaction* sendRequest(const string& url, SData& request);
};
