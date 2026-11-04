#include "libstuff/SHTTPSManager.h"
#include "libstuff/STCPManager.h"
#include "test/lib/tpunit++.hpp"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
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

    static string progress(const Transaction& transaction)
    {
        return _describeRequestProgress(transaction);
    }
};

struct SSLTest : tpunit::TestFixture
{
    SSLTest()
        : tpunit::TestFixture("SSL",
                              BEFORE_CLASS(SSLTest::setup),
                              TEST(SSLTest::test),
                              TEST(SSLTest::proxyTest),
                              TEST(SSLTest::closedProxyTunnelTest),
                              TEST(SSLTest::failureStageTest),
                              TEST(SSLTest::failureFinishesTransactionTest),
                              TEST(SSLTest::streamingResponseTest),
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

    void closedProxyTunnelTest()
    {
        SStandaloneHTTPSManager manager;
        for (const bool proxyRejected : {false, true}) {
            // Given a closed proxy socket that never opened a tunnel to the target.
            SStandaloneHTTPSManager::Transaction transaction(manager);
            transaction.fullRequest.methodLine = "HEAD / HTTP/1.1";
            transaction.s = new SHTTPSProxySocket("127.0.0.1:9", "example.com:443", "");
            transaction.s->shutdown(STCPManager::Socket::CLOSED);
            transaction.s->recvBuffer = proxyRejected
                ? "HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 5\r\n\r\nproxy"
                : "HTTP/1.1 200 \r\n\r\n";

            // When the manager polls the remaining CONNECT response.
            fd_map fdm;
            uint64_t nextActivity = STimeNow();
            manager.postPoll(fdm, transaction, nextActivity);

            // Then a rejection is reported with its body, and a 200 without a tunnel is a transport failure.
            ASSERT_TRUE(transaction.finished);
            ASSERT_FALSE(transaction.s);
            ASSERT_EQUAL(transaction.response, proxyRejected ? 407 : 501);
            ASSERT_EQUAL(transaction.fullResponse.content, proxyRejected ? "proxy" : "");
        }
    }

    void streamingResponseTest()
    {
        SStandaloneHTTPSManager manager;
        using State = STCPManager::Socket::State;
        const string header = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
        const auto pollTransaction = [&](SStandaloneHTTPSManager::Transaction& transaction) {
            fd_map fdm;
            uint64_t nextActivity = STimeNow() + 1'000'000;
            manager.prePoll(fdm, transaction);
            S_poll(fdm, 0);
            manager.postPoll(fdm, transaction, nextActivity);
        };

        for (const bool shouldStreamResponse : {false, true}) {
            // Given an open transaction that reads a chunked response.
            int sockets[2];
            ASSERT_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
            STCPManager::Socket peer(sockets[1], State::CONNECTED);
            SStandaloneHTTPSManager::Transaction transaction(manager);
            transaction.shouldStreamResponse = shouldStreamResponse;
            transaction.s = new STCPManager::Socket(sockets[0], State::CONNECTED);
            transaction.timeoutAt = STimeNow() + 5'000'000;

            // When a complete chunk arrives before the response terminates.
            ASSERT_TRUE(peer.send(header + "5\r\nhello\r\n"));
            pollTransaction(transaction);

            // Then only the streaming caller can see the body while the transaction is pending.
            ASSERT_EQUAL(transaction.response, 0);
            ASSERT_FALSE(transaction.finished);
            ASSERT_EQUAL(transaction.fullResponse.content, shouldStreamResponse ? "hello" : "");

            // When a later chunk and the terminator arrive on the same socket.
            ASSERT_TRUE(peer.send("6\r\n world\r\n0\r\n\r\n"));
            pollTransaction(transaction);

            // Then both callers get the same complete response without repeated body bytes.
            ASSERT_EQUAL(transaction.response, 200);
            ASSERT_TRUE(transaction.finished);
            ASSERT_EQUAL(transaction.fullResponse.content, "hello world");
        }

        for (const bool shouldClose : {false, true}) {
            // Given a streaming transaction with a partially received chunk.
            int sockets[2];
            ASSERT_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
            STCPManager::Socket peer(sockets[1], State::CONNECTED);
            SStandaloneHTTPSManager::Transaction transaction(manager);
            transaction.shouldStreamResponse = true;
            transaction.s = new STCPManager::Socket(sockets[0], State::CONNECTED);
            transaction.timeoutAt = STimeNow() + 5'000'000;
            ASSERT_TRUE(peer.send(header + "5\r\nhe"));
            pollTransaction(transaction);
            ASSERT_EQUAL(transaction.fullResponse.content, "he");
            ASSERT_EQUAL(transaction.response, 0);

            // When the connection closes or the deadline passes before the chunk completes.
            if (shouldClose) {
                ASSERT_EQUAL(::shutdown(peer.s, SHUT_WR), 0);
            } else {
                transaction.timeoutAt = STimeNow() - 1;
            }
            pollTransaction(transaction);

            // Then parsed success headers cannot turn the interrupted stream into HTTP 200.
            ASSERT_EQUAL(transaction.response, 501);
            ASSERT_EQUAL(transaction.fullResponse.methodLine, "HTTP/1.1 200 OK");
            ASSERT_EQUAL(transaction.fullResponse.content, "he");
            ASSERT_TRUE(transaction.finished);
            ASSERT_FALSE(transaction.s);
        }

        int sockets[2];
        ASSERT_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
        STCPManager::Socket peer(sockets[1], State::CONNECTED);
        SStandaloneHTTPSManager::Transaction transaction(manager);
        transaction.shouldStreamResponse = true;
        transaction.s = new STCPManager::Socket(sockets[0], State::CONNECTED);
        transaction.timeoutAt = STimeNow() + 5'000'000;
        ASSERT_TRUE(peer.send("HTTP/1.1 200 OK\r\nContent-Ty"));
        pollTransaction(transaction);
        ASSERT_EQUAL(transaction.response, 0);
        ASSERT_EQUAL(::shutdown(peer.s, SHUT_WR), 0);
        pollTransaction(transaction);

        ASSERT_EQUAL(transaction.response, 501);
        ASSERT_TRUE(transaction.finished);
        ASSERT_FALSE(transaction.s);
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
        // A failed lookup never fills in the address; a failed connect has one.
        direct.s->connectFailure = true;
        EXPECT_EQUAL(FailureStageHTTPSManager::describe(direct, State::CLOSED, false), "resolving DNS");
        direct.s->addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
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

        // An empty send buffer alone isn't "sent": the request only counts once bytes have actually gone out, or for a
        // proxy, once the tunnel is open.
        EXPECT_EQUAL(FailureStageHTTPSManager::progress(direct), "not sent");
        direct.s->lastSendTime = direct.s->openTime + 1;
        EXPECT_EQUAL(FailureStageHTTPSManager::progress(direct), "sent");
        direct.s->setSendBuffer("rest of the request");
        EXPECT_EQUAL(FailureStageHTTPSManager::progress(direct), "not fully sent");
        EXPECT_EQUAL(FailureStageHTTPSManager::progress(proxied), "not sent");
    }

    void failureFinishesTransactionTest()
    {
        // Find a localhost port with nothing listening on it, so the connection is refused.
        const int fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        socklen_t length = sizeof(addr);
        ::bind(fd, (sockaddr*) &addr, sizeof(addr));
        getsockname(fd, (sockaddr*) &addr, &length);
        close(fd);
        const string host = "127.0.0.1:" + to_string(ntohs(addr.sin_port));

        for (const bool shouldStreamResponse : {false, true}) {
            SStandaloneHTTPSManager manager;
            SStandaloneHTTPSManager::Transaction transaction(manager);
            transaction.shouldStreamResponse = shouldStreamResponse;
            transaction.targetHost = host;
            transaction.timeoutAt = STimeNow() + 5'000'000;
            transaction.s = new STCPManager::Socket(host, false);
            transaction.s->send(SData("GET / HTTP/1.1").serialize());

            while (!transaction.response) {
                fd_map fdm;
                uint64_t nextActivity = STimeNow();
                manager.prePoll(fdm, transaction);
                S_poll(fdm, 100'000);
                manager.postPoll(fdm, transaction, nextActivity);
            }

            // A failed transaction is finished and its socket is freed, so polling it again doesn't report the failure
            // again.
            EXPECT_EQUAL(transaction.response, 500);
            EXPECT_TRUE(transaction.finished);
            EXPECT_FALSE(transaction.s);
        }
    }
} __SSLTest;
