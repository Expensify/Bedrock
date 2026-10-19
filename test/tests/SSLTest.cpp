#include "libstuff/SHTTPSManager.h"
#include "libstuff/STCPManager.h"
#include "test/lib/tpunit++.hpp"
#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <sqlitecluster/SQLiteNode.h>
#include <test/lib/BedrockTester.h>
#include <libstuff/SHTTPSProxySocket.h>

// Exposes the failure-stage description so it can be checked without a real network failure.
class FailureStageHTTPSManager : public SStandaloneHTTPSManager
{
public:
    static string describe(const Transaction& transaction, STCPManager::Socket::State state, bool requestSent)
    {
        return _describeFailureStage(transaction, state, requestSent);
    }
};

struct SSLTest : tpunit::TestFixture
{
    SSLTest()
        : tpunit::TestFixture("SSL",
                              BEFORE_CLASS(SSLTest::setup),
                              TEST(SSLTest::test),
                              TEST(SSLTest::proxyTest),
                              TEST(SSLTest::failureStageTest),
                              AFTER_CLASS(SSLTest::teardown))
    {
    }

    BedrockTester* tester;

    void setup()
    {
        char cwd[1024];
        if (!getcwd(cwd, sizeof(cwd))) {
            STHROW("Couldn't get CWD");
        }

        tester = new BedrockTester({
            {"-plugins", string(cwd) + "/clustertest/testplugin/testplugin.so"},
        });
    }

    void teardown()
    {
        delete tester;
    }

    void test()
    {
        for (auto& url : (map<string, string>){
            // Verify we get some HTTP response from google. We don't care what it is, just that it's valid
            // HTTP. We want to notice that our fake URL, fails, though.
            {"www.google.com", "HTTP/1.1"},
            {"www.notarealplaceforsure.com.fake", "NO_RESPONSE"},
        }) {
            SData request("sendrequest");
            request["Host"] = url.first;
            request["Connection"] = "Close";
            request["passthrough"] = "true";
            // Note: the fake URL is known to periodically time out. (after 60s) Give it enough time for one retry.
            tester->executeWaitVerifyContent({request}, url.second, false, 70'000'000);
        }
    }

    void proxyTest()
    {
        // This is a generic HTTPS manager.
        SStandaloneHTTPSManager manager;

        const string host = "example.com:443";
        SData request("GET / HTTP/1.1");
        request["host"] = host;

        // Note: this works with a default squid install, which Github actions doesn't currently have.
        const string proxy = "127.0.0.1:3128";

        // Create a transaction with a socket, send the above request.
        unique_ptr<SStandaloneHTTPSManager::Transaction> transaction = make_unique<SStandaloneHTTPSManager::Transaction>(manager, "proxyTest");

        // Verify requestID is set correctly on the transaction
        EXPECT_EQUAL(transaction->requestID, "proxyTest");

        transaction->s = new SHTTPSProxySocket(proxy, host, transaction->requestID);
        transaction->timeoutAt = STimeNow() + 5'000'000;
        transaction->s->send(request.serialize());

        // Wait for a response.
        while (!transaction->response) {
            fd_map fdm;
            uint64_t nextActivity = STimeNow();
            manager.prePoll(fdm, *transaction);
            S_poll(fdm, 1'000'000);
            manager.postPoll(fdm, *transaction, nextActivity);
        }

        // Validate that the response is reasonable
        EXPECT_EQUAL(transaction->response, 200);

        // Make sure that the response has a body. This differentiates it from the response to a CONNECT message
        // So that we can test we're looking at the actual proxied response and not just the response from the proxy itself.
        EXPECT_TRUE(transaction->fullResponse.content.size());
    }

    void failureStageTest()
    {
        FailureStageHTTPSManager manager;
        using State = STCPManager::Socket::State;

        // Direct connection: each stage before a response is described, and the request is only "sent" once its
        // send buffer has drained.
        SStandaloneHTTPSManager::Transaction direct(manager);
        direct.targetHost = "example.com:443";
        direct.s = new STCPManager::Socket(-1, State::CONNECTING);
        EXPECT_EQUAL(FailureStageHTTPSManager::describe(direct, State::RESOLVING, false), "resolving DNS");
        EXPECT_EQUAL(FailureStageHTTPSManager::describe(direct, State::CONNECTING, false), "connecting");
        direct.s->connectFailure = true;
        EXPECT_EQUAL(FailureStageHTTPSManager::describe(direct, State::CLOSED, false), "connecting");
        direct.s->connectFailure = false;
        EXPECT_EQUAL(FailureStageHTTPSManager::describe(direct, State::CONNECTED, false), "sending the request");
        EXPECT_EQUAL(FailureStageHTTPSManager::describe(direct, State::CONNECTED, true), "waiting for a response");

        // Through a proxy: failures before the CONNECT tunnel is open are described as proxy steps, since nothing
        // has reached the target host yet.
        SStandaloneHTTPSManager::Transaction proxied(manager);
        proxied.targetHost = "example.com:443";
        proxied.proxyHost = "127.0.0.1:9";
        proxied.s = new SHTTPSProxySocket("127.0.0.1:9", "example.com:443", "");
        EXPECT_EQUAL(FailureStageHTTPSManager::describe(proxied, State::RESOLVING, false), "resolving the proxy");
        EXPECT_EQUAL(FailureStageHTTPSManager::describe(proxied, State::CONNECTING, false), "connecting to the proxy");
        proxied.s->connectFailure = false;
        EXPECT_EQUAL(FailureStageHTTPSManager::describe(proxied, State::CLOSED, false), "opening the proxy tunnel");
    }
} __SSLTest;
