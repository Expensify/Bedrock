/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SSLTest.cpp
 * Path:    test/tests/SSLTest.cpp
 *
 * INTENT
 *   Integration tests for outbound HTTPS handling: that a bedrock node
 *   can proxy a "sendrequest" command to a real external site over TLS
 *   (and correctly reports failure for an unresolvable host), and that
 *   SStandaloneHTTPSManager/SHTTPSProxySocket can drive a request through
 *   an HTTP CONNECT-style proxy to completion.
 *
 * OBJECTS
 *   SSLTest  - tpunit::TestFixture; `test` spins up a BedrockTester with
 *              the clustertest test plugin and issues passthrough requests
 *              to google.com and a deliberately-fake host; `proxyTest`
 *              builds an SStandaloneHTTPSManager::Transaction directly,
 *              attaches an SHTTPSProxySocket pointed at a local squid
 *              proxy, and hand-pumps prePoll/postPoll until a response
 *              arrives.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits: an SSL/HTTPS-focused test alongside the other command/protocol
 *   tests in test/tests.
 *
 * NAMING QUALITY
 *   Consistent with sibling fixtures. `proxyTest` depends on a local
 *   squid install (noted in-code as unavailable on GitHub Actions), which
 *   is an environment fragility rather than a naming issue.
 * ─────────────────────────────────────────────────────────────────────*/

#include "libstuff/SHTTPSManager.h"
#include "libstuff/STCPManager.h"
#include "test/lib/tpunit++.hpp"
#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <sqlitecluster/SQLiteNode.h>
#include <test/lib/BedrockTester.h>
#include <libstuff/SHTTPSProxySocket.h>

struct SSLTest : tpunit::TestFixture
{
    SSLTest()
        : tpunit::TestFixture("SSL",
                              BEFORE_CLASS(SSLTest::setup),
                              TEST(SSLTest::test),
                              TEST(SSLTest::proxyTest),
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
} __SSLTest;
