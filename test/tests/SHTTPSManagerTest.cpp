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
    using HTTPSProxySocketFactory = SStandaloneHTTPSManager::HTTPSProxySocketFactory;

    unique_ptr<Transaction> sendNow(const string& url, const SData& request, bool allowProxy = false)
    {
        return _httpsSend(url, request, allowProxy);
    }

    unique_ptr<Transaction> sendAt(const string& url, const SData& request, uint64_t scheduledStartUS, bool allowProxy = false)
    {
        return _httpsSendAt(url, request, scheduledStartUS, allowProxy);
    }

    unique_ptr<Transaction> sendWithProxyFactory(
        const string& url,
        const SData& request,
        bool allowProxy,
        const string& proxyAddress,
        const HTTPSProxySocketFactory& createProxySocket)
    {
        auto transaction = make_unique<Transaction>(*this);
        _startHTTPSRequest(*transaction, url, request, allowProxy, proxyAddress, createProxySocket);
        return transaction;
    }

    void startWithProxyFactory(
        Transaction& transaction,
        const string& url,
        const SData& request,
        bool allowProxy,
        const string& proxyAddress,
        const HTTPSProxySocketFactory& createProxySocket)
    {
        _startHTTPSRequest(transaction, url, request, allowProxy, proxyAddress, createProxySocket);
    }
};

struct ProxyFactoryCapture
{
    struct Call
    {
        string proxyAddress;
        string host;
        string requestID;
    };

    vector<string> sentRequests;
    vector<Call> calls;
    bool fail = false;

    unique_ptr<STCPManager::Socket> create(const string& proxyAddress, const string& host, const string& requestID)
    {
        calls.push_back({proxyAddress, host, requestID});
        if (fail) {
            STHROW("500 Test proxy construction failure");
        }
        return make_unique<CapturingSocket>(sentRequests);
    }
};

struct SHTTPSManagerTest : tpunit::TestFixture
{
    SHTTPSManagerTest()
        : tpunit::TestFixture("SHTTPSManager",
                              TEST(SHTTPSManagerTest::testFutureRequestIsSocketless),
                              TEST(SHTTPSManagerTest::testDueRequestUsesImmediatePath),
                              TEST(SHTTPSManagerTest::testScheduledSetupFailureCompletes),
                              TEST(SHTTPSManagerTest::testScheduledSerializationFailureCompletes),
                              TEST(SHTTPSManagerTest::testProxyFactoryFailureCompletes),
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
    }

    void testScheduledSerializationFailureCompletes()
    {
        ExposedHTTPSManager manager;
        SData invalidRequest("GET\n");
        auto serializationFailure = manager.sendAt("https://example.com/", invalidRequest, STimeNow() + 100'000);

        ASSERT_NO_THROW(serializationFailure->startFunc(*serializationFailure));

        ASSERT_TRUE(serializationFailure->s == nullptr);
        ASSERT_EQUAL(serializationFailure->response, 503);
        ASSERT_TRUE(serializationFailure->finished != 0);
    }

    void testProxyFactoryFailureCompletes()
    {
        ExposedHTTPSManager proxyManager;
        ProxyFactoryCapture proxyFactory;
        proxyFactory.fail = true;
        SData request("GET / HTTP/1.1");
        request["Host"] = "example.com";
        ExposedHTTPSManager::HTTPSProxySocketFactory failingFactory = [&proxyFactory](const string& proxyAddress, const string& host, const string& requestID) {
            return proxyFactory.create(proxyAddress, host, requestID);
        };
        auto proxyFailure = proxyManager.sendWithProxyFactory(
            "https://example.com/", request, true, "https://proxy.example.com:443", failingFactory);

        ASSERT_TRUE(proxyFailure->s == nullptr);
        ASSERT_EQUAL(proxyFailure->response, 503);
        ASSERT_TRUE(proxyFailure->finished != 0);
    }

    void testProxyRequestParity()
    {
        ExposedHTTPSManager manager;
        ProxyFactoryCapture proxyFactory;
        ExposedHTTPSManager::HTTPSProxySocketFactory factory = [&proxyFactory](const string& proxyAddress, const string& host, const string& requestID) {
            return proxyFactory.create(proxyAddress, host, requestID);
        };
        SData request("GET / HTTP/1.1");
        request["Host"] = "example.com";
        request["Connection"] = "close";

        auto immediate = manager.sendWithProxyFactory(
            "https://example.com/", request, true, "https://proxy.example.com:443", factory);
        auto deferred = manager.sendAt("https://example.com/", request, STimeNow() + 100'000, true);

        ASSERT_TRUE(deferred->s == nullptr);
        ASSERT_EQUAL(deferred->fullRequest["Connection"], "close");

        manager.startWithProxyFactory(
            *deferred, "https://example.com/", deferred->fullRequest, true, "https://proxy.example.com:443", factory);

        ASSERT_EQUAL(proxyFactory.sentRequests.size(), 2);
        ASSERT_EQUAL(proxyFactory.calls.size(), 2);
        ASSERT_EQUAL(proxyFactory.calls[0].proxyAddress, "proxy.example.com:443");
        ASSERT_EQUAL(proxyFactory.calls[0].host, "example.com:443");
        ASSERT_FALSE(proxyFactory.calls[0].requestID.empty());
        ASSERT_TRUE(proxyFactory.sentRequests[0].find("Connection") == string::npos);
        ASSERT_TRUE(immediate->fullRequest["Connection"].empty());
        ASSERT_TRUE(deferred->fullRequest["Connection"].empty());
        ASSERT_EQUAL(proxyFactory.calls[1].proxyAddress, proxyFactory.calls[0].proxyAddress);
        ASSERT_EQUAL(proxyFactory.calls[1].host, proxyFactory.calls[0].host);
        ASSERT_EQUAL(proxyFactory.calls[1].requestID, proxyFactory.calls[0].requestID);
        ASSERT_TRUE(proxyFactory.sentRequests[1].find("Connection") == string::npos);
    }
} __SHTTPSManagerTest;
