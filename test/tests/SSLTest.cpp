#include "libstuff/SHTTPSManager.h"
#include "libstuff/STCPManager.h"
#include "test/lib/tpunit++.hpp"
#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <sqlitecluster/SQLiteNode.h>
#include <test/lib/BedrockTester.h>

struct SSLTest : tpunit::TestFixture {
    SSLTest()
        : tpunit::TestFixture("SSL",
                              BEFORE_CLASS(SSLTest::setup),
                              TEST(SSLTest::proxyTest),
                              TEST(SSLTest::test),
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
        // This is a generic HTTPS mamanger.
        SStandaloneHTTPSManager manager;

        const string host = "www.example.com:443";
        SData request("GET " + host + " HTTP/1.1");
        request["host"] = host;

        // Create a transaction with a socket, send the above request.
        SStandaloneHTTPSManager::Transaction* transaction = new SStandaloneHTTPSManager::Transaction(manager);
        transaction->s = new STCPManager::Socket(host, true);
        transaction->timeoutAt = STimeNow() + 5'000'000;
        transaction->s->send(request.serialize());

        // Wait for a response.
        while (!transaction->response) {
            fd_map fdm;
            uint64_t nextActivity = STimeNow();
            manager.prePoll(fdm, *transaction);
            S_poll(fdm, 1'000);
            manager.postPoll(fdm, *transaction, nextActivity);
        }

        // Validate that the response is reasonable
        cout << transaction->fullResponse.serialize() << endl;
        ASSERT_EQUAL(transaction->response, 200);

        // Close the transaction.
        manager.closeTransaction(transaction);
    }
} __SSLTest;
