#include "libstuff/SHTTPSManager.h"
#include "libstuff/SData.h"
#include "test/lib/tpunit++.hpp"

class CapturingSocket : public STCPManager::Socket
{
public:
    explicit CapturingSocket(vector<string>& sentRequests_)
        : STCPManager::Socket(-1, STCPManager::Socket::CONNECTED), sentRequests(sentRequests_)
    {
    }

    bool send(const string& buffer, size_t* bytesSentCount = nullptr) override
    {
        sentRequests.push_back(buffer);
        if (bytesSentCount) {
            *bytesSentCount = buffer.size();
        }
        return true;
    }

private:
    vector<string>& sentRequests;
};

class ExposedHTTPSManager : public SStandaloneHTTPSManager
{
public:
    unique_ptr<Transaction> sendNow(const string& url, const SData& request, bool allowProxy = false)
    {
        return _httpsSend(url, request, allowProxy);
    }

    unique_ptr<Transaction> sendAt(const string& url, const SData& request, uint64_t scheduledStartUS, bool allowProxy = false)
    {
        return _httpsSendAt(url, request, scheduledStartUS, allowProxy);
    }
};

class ProxyTestHTTPSManager : public ExposedHTTPSManager
{
public:
    vector<string> sentRequests;
    bool failProxy = false;

protected:
    const string& _getProxyAddressHTTPS() const override
    {
        return proxyAddress;
    }

    STCPManager::Socket* _createHTTPSProxySocket(const string&, const string&, const string&) override
    {
        if (failProxy) {
            STHROW("500 Test proxy construction failure");
        }
        return new CapturingSocket(sentRequests);
    }

private:
    const string proxyAddress = "proxy.example.com:443";
};

struct SHTTPSManagerTest : tpunit::TestFixture
{
    SHTTPSManagerTest()
        : tpunit::TestFixture("SHTTPSManager",
                              TEST(SHTTPSManagerTest::testFutureRequestIsSocketless),
                              TEST(SHTTPSManagerTest::testDueRequestUsesImmediatePath),
                              TEST(SHTTPSManagerTest::testScheduledSetupFailureCompletes),
                              TEST(SHTTPSManagerTest::testProxyRequestParity))
    {
    }

    void testFutureRequestIsSocketless()
    {
        ExposedHTTPSManager manager;
        SData request("GET / HTTP/1.1");
        request["Host"] = "example.com";
        request["X-Test-Marker"] = "deferred";
        request.content = "deferred-body";

        uint64_t scheduledStartUS = STimeNow() + 100'000;
        auto transaction = manager.sendAt("https://example.com/", request, scheduledStartUS);

        ASSERT_TRUE(transaction->s == nullptr);
        ASSERT_EQUAL(transaction->response, 0);
        ASSERT_EQUAL(transaction->scheduledStart, scheduledStartUS);
        ASSERT_TRUE(transaction->startFunc);
        ASSERT_EQUAL(transaction->fullRequest["X-Test-Marker"], "deferred");
        ASSERT_EQUAL(transaction->fullRequest.content, "deferred-body");
    }

    void testDueRequestUsesImmediatePath()
    {
        ExposedHTTPSManager manager;
        SData request("GET / HTTP/1.1");
        request["Host"] = "example.com";

        auto immediate = manager.sendNow("not a URI", request);
        auto zero = manager.sendAt("not a URI", request, 0);
        auto past = manager.sendAt("not a URI", request, STimeNow() - 1);
        auto current = manager.sendAt("not a URI", request, STimeNow());

        for (const auto& transaction : {immediate.get(), zero.get(), past.get(), current.get()}) {
            ASSERT_TRUE(transaction->s == nullptr);
            ASSERT_EQUAL(transaction->response, 503);
            ASSERT_TRUE(transaction->finished != 0);
            ASSERT_EQUAL(transaction->scheduledStart, 0);
            ASSERT_FALSE(transaction->startFunc);
        }
    }

    void testScheduledSetupFailureCompletes()
    {
        ExposedHTTPSManager manager;
        SData request("GET / HTTP/1.1");

        auto transaction = manager.sendAt("not a URI", request, STimeNow() + 100'000);
        ASSERT_TRUE(transaction->s == nullptr);
        ASSERT_EQUAL(transaction->response, 0);

        ASSERT_NO_THROW(transaction->startFunc(*transaction));

        ASSERT_TRUE(transaction->s == nullptr);
        ASSERT_EQUAL(transaction->response, 503);
        ASSERT_TRUE(transaction->finished != 0);

        ProxyTestHTTPSManager proxyManager;
        proxyManager.failProxy = true;
        request["Host"] = "example.com";
        auto proxyFailure = proxyManager.sendAt("https://example.com/", request, STimeNow() + 100'000, true);

        ASSERT_NO_THROW(proxyFailure->startFunc(*proxyFailure));

        ASSERT_TRUE(proxyFailure->s == nullptr);
        ASSERT_EQUAL(proxyFailure->response, 503);
        ASSERT_TRUE(proxyFailure->finished != 0);
    }

    void testProxyRequestParity()
    {
        ProxyTestHTTPSManager manager;
        SData request("GET / HTTP/1.1");
        request["Host"] = "example.com";
        request["Connection"] = "close";

        auto immediate = manager.sendNow("https://example.com/", request, true);
        auto deferred = manager.sendAt("https://example.com/", request, STimeNow() + 100'000, true);

        ASSERT_EQUAL(manager.sentRequests.size(), 1);
        ASSERT_TRUE(manager.sentRequests[0].find("Connection") == string::npos);
        ASSERT_TRUE(immediate->fullRequest["Connection"].empty());
        ASSERT_EQUAL(deferred->fullRequest["Connection"], "close");

        deferred->startFunc(*deferred);

        ASSERT_EQUAL(manager.sentRequests.size(), 2);
        ASSERT_TRUE(manager.sentRequests[1].find("Connection") == string::npos);
        ASSERT_TRUE(deferred->fullRequest["Connection"].empty());
    }
} __SHTTPSManagerTest;
