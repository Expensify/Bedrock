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
                              TEST(SSLTest::test),
                              TEST(SSLTest::proxyTest),
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
        SStandaloneHTTPSManager manager;

        // Specifically validates that we properly detect SSL certificate/hostname mismatches
        const string host = "wrong.host.badssl.com:443";
        SData request("GET / HTTP/1.1");
        request["host"] = host;

        // Create a transaction with a socket, send the above request.
        SStandaloneHTTPSManager::Transaction* transaction = new SStandaloneHTTPSManager::Transaction(manager);
        // We are attempting to test a ertificate mismatch, but occasionally, we fail to connect at all, which is not the
        // case we care about here. We allow several retries when that happens.
        // If this failure continues past 5 tries, the test will still end up failing as the socket is not connected.
        for (int i = 0; i < 5; i++) {
            try {
                transaction->s = new STCPManager::Socket(host, true);
                break;
            } catch (SException e) {
                // Try again in a second.
                sleep (1);
            }
        }

        transaction->timeoutAt = STimeNow() + 10'000'000;
        transaction->s->send(request.serialize());

        while (!transaction->response && (STimeNow() < transaction->timeoutAt)) {
            fd_map fdm;
            uint64_t nextActivity = STimeNow();
            manager.prePoll(fdm, *transaction);
            S_poll(fdm, 1'000'000);
            manager.postPoll(fdm, *transaction, nextActivity);
        }

        // We expect the connection to fail due to certificate validation error
        // The wrong.host.badssl.com site has a certificate that doesn't match the hostname
        // A successful connection with a 2xx or 3xx response indicates SSL validation passed
        ASSERT_TRUE(transaction->response >= 400);

        // Close the transaction
        manager.closeTransaction(transaction);
    }
} __SSLTest;
