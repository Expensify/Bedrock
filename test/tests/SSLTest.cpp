#include "libstuff/SHTTPSManager.h"
#include "libstuff/STCPManager.h"
#include "test/lib/tpunit++.hpp"
#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <sqlitecluster/SQLiteNode.h>
#include <test/lib/BedrockTester.h>
#include <libstuff/SHTTPSProxySocket.h>

struct SSLTest : tpunit::TestFixture {
    SSLTest()
        : tpunit::TestFixture("SSL",
                              BEFORE_CLASS(SSLTest::setup),
                              TEST(SSLTest::proxyTest),
                              TEST(SSLTest::test),
                              TEST(SSLTest::certificateValidationTest),
                              AFTER_CLASS(SSLTest::teardown))
    { }

    BedrockTester* tester;

    void setup() {
        char cwd[1024];
        if (!getcwd(cwd, sizeof(cwd))) {
            STHROW("Couldn't get CWD");
        }

        tester = new BedrockTester({
            {"-plugins", string(cwd) + "/clustertest/testplugin/testplugin.so"},
        });
    }

    void teardown() {
        delete tester;
    }

    void test() {
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

    void proxyTest() {
        // This is a generic HTTPS manager.
        SStandaloneHTTPSManager manager;

        const string host = "example.com:443";
        SData request("GET / HTTP/1.1");
        request["host"] = host;

        // Note: this works with a default squid install, which Github actions doesn't currently have.
        const string proxy = "127.0.0.1:3128";

        // Create a transaction with a socket, send the above request.
        SStandaloneHTTPSManager::Transaction* transaction = new SStandaloneHTTPSManager::Transaction(manager);
        transaction->s = new SHTTPSProxySocket(proxy, host);
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
        ASSERT_EQUAL(transaction->response, 200);

        // Make sure that the response has a body. This differentiates it from the response to a CONNECT message
        // So that we can test we're looking at the actual proxied response and not just the response from the proxy itself.
        ASSERT_TRUE(transaction->fullResponse.content.size());

        // Close the transaction.
        manager.closeTransaction(transaction);
    }

    void certificateValidationTest() {
        // This test verifies that we properly detect SSL certificate/hostname mismatches
        SStandaloneHTTPSManager manager;

        const string host = "wrong.host.badssl.com:443";
        SData request("GET / HTTP/1.1");
        request["host"] = host;

        // Create a transaction with a socket, send the above request.
        SStandaloneHTTPSManager::Transaction* transaction = new SStandaloneHTTPSManager::Transaction(manager);
        transaction->s = new STCPManager::Socket(host, true); // true for HTTPS
        transaction->timeoutAt = STimeNow() + 10'000'000; // 10 second timeout
        transaction->s->send(request.serialize());

        // Wait for a response or connection failure
        bool connectionFailed = false;
        while (!transaction->response && !connectionFailed) {
            fd_map fdm;
            uint64_t nextActivity = STimeNow();
            manager.prePoll(fdm, *transaction);
            S_poll(fdm, 1'000'000);
            manager.postPoll(fdm, *transaction, nextActivity);

            // Check if the connection failed due to SSL certificate validation
            if (transaction->s && transaction->s->state == STCPManager::Socket::CLOSED) {
                connectionFailed = true;
            }

            // Check for timeout
            if (STimeNow() > transaction->timeoutAt) {
                connectionFailed = true;
            }
        }

        // We expect the connection to fail due to certificate validation error
        // The wrong.host.badssl.com site has a certificate that doesn't match the hostname
        ASSERT_TRUE(connectionFailed || transaction->response >= 400);

        // Close the transaction
        manager.closeTransaction(transaction);
    }
} __SSLTest;
