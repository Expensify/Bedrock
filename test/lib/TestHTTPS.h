#pragma once
#include <libstuff/libstuff.h>
#include <libstuff/SHTTPSManager.h>

class TestHTTPS : public SHTTPSManager {
public:
    TestHTTPS(BedrockPlugin& plugin_) : SHTTPSManager(plugin_)
    {
    }

    virtual ~TestHTTPS();

    // SHTTPSManager API
    virtual bool _onRecv(Transaction* transaction);
    virtual Transaction* sendRequest(const string& url, SData& request);
};
