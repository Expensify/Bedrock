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

    unique_ptr<Transaction> send(const string& url, const SData& request, int maxReconnectAttempts = MAX_RECONNECT_ATTEMPTS)
    {
        return _httpsSend(url, request, false, maxReconnectAttempts);
    }
};

// A localhost listener that the test answers between polls, so it needs no thread of its own. The kernel completes
// connections before they're accepted, so a request sent to it has been sent even if it's never answered.
class FakeHTTPServer
{
public:
    FakeHTTPServer(uint16_t port)
    {
        listenFD = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        sockaddr_in addr = loopback(port);
        if (::bind(listenFD, (sockaddr*) &addr, sizeof(addr)) || listen(listenFD, 8)) {
            STHROW("Couldn't listen on port " + to_string(port));
        }
    }

    ~FakeHTTPServer()
    {
        for (const auto& [fd, received] : requests) {
            close(fd);
        }
        close(listenFD);
    }

    // Accepts new connections, and answers each one whose request headers have arrived with `reply`. An empty reply
    // leaves them open and unanswered.
    void answer(const string& reply)
    {
        for (int fd = accept4(listenFD, nullptr, nullptr, SOCK_NONBLOCK); fd >= 0; fd = accept4(listenFD, nullptr, nullptr, SOCK_NONBLOCK)) {
            connections++;
            requests[fd] = "";
        }
        for (auto it = requests.begin(); it != requests.end();) {
            char buffer[4096];
            const ssize_t count = ::recv(it->first, buffer, sizeof(buffer), 0);
            if (count > 0) {
                it->second.append(buffer, count);
            }
            if (reply.empty() || !SContains(it->second, "\r\n\r\n")) {
                it++;
                continue;
            }
            ::send(it->first, reply.c_str(), reply.size(), MSG_NOSIGNAL);
            close(it->first);
            it = requests.erase(it);
        }
    }

    // Returns a localhost port with nothing listening on it.
    static uint16_t unusedPort()
    {
        const int fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr = loopback(0);
        socklen_t length = sizeof(addr);
        ::bind(fd, (sockaddr*) &addr, sizeof(addr));
        getsockname(fd, (sockaddr*) &addr, &length);
        close(fd);
        return ntohs(addr.sin_port);
    }

    int connections = 0;

private:
    static sockaddr_in loopback(uint16_t port)
    {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        return addr;
    }

    int listenFD = -1;
    map<int, string> requests;
};

struct SSLTest : tpunit::TestFixture
{
    SSLTest()
        : tpunit::TestFixture("SSL",
                              BEFORE_CLASS(SSLTest::setup),
                              TEST(SSLTest::test),
                              TEST(SSLTest::proxyTest),
                              TEST(SSLTest::failureStageTest),
                              TEST(SSLTest::failureFinishesTransactionTest),
                              TEST(SSLTest::reconnectTest),
                              TEST(SSLTest::proxyRefusedReconnectTest),
                              TEST(SSLTest::noReconnectAfterSendTest),
                              TEST(SSLTest::reconnectOptOutTest),
                              TEST(SSLTest::reconnectAfterTimeoutTest),
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

    // Polls the transaction until it has a response. `onPoll` runs after each poll.
    static void waitForResponse(SStandaloneHTTPSManager& manager, SStandaloneHTTPSManager::Transaction& transaction,
                                uint64_t timeoutMS = 300'000, const function<void()>& onPoll = [] (){})
    {
        while (!transaction.response) {
            fd_map fdm;
            uint64_t nextActivity = STimeNow();
            manager.prePoll(fdm, transaction);
            S_poll(fdm, 100'000);
            manager.postPoll(fdm, transaction, nextActivity, timeoutMS);
            onPoll();
        }
    }

    void failureFinishesTransactionTest()
    {
        // Nothing listens on this port, so the connection is refused. The transaction isn't set up to reconnect.
        const string host = "127.0.0.1:" + to_string(FakeHTTPServer::unusedPort());
        SStandaloneHTTPSManager manager;
        SStandaloneHTTPSManager::Transaction transaction(manager);
        transaction.targetHost = host;
        transaction.timeoutAt = STimeNow() + 5'000'000;
        transaction.s = new STCPManager::Socket(host, false);
        transaction.s->send(SData("GET / HTTP/1.1").serialize());

        waitForResponse(manager, transaction);

        // A failed transaction is finished and its socket is freed, so polling it again doesn't report the failure
        // again.
        EXPECT_EQUAL(transaction.response, 500);
        EXPECT_TRUE(transaction.finished);
        EXPECT_FALSE(transaction.s);
    }

    void reconnectTest()
    {
        // The first connection is refused because nothing is listening yet. The server starts once the reconnect is
        // scheduled, so the retry gets the response.
        FailureStageHTTPSManager manager;
        const uint16_t port = FakeHTTPServer::unusedPort();
        SData request("GET / HTTP/1.1");
        request["Host"] = "127.0.0.1:" + to_string(port);
        unique_ptr<SStandaloneHTTPSManager::Transaction> transaction = manager.send("http://127.0.0.1:" + to_string(port) + "/", request);
        transaction->timeoutAt = STimeNow() + 10'000'000;

        unique_ptr<FakeHTTPServer> server;
        waitForResponse(manager, *transaction, 300'000, [&]() {
            if (!server && transaction->reconnectAt) {
                server = make_unique<FakeHTTPServer>(port);
            }
            if (server) {
                server->answer("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
            }
        });

        EXPECT_EQUAL(transaction->response, 200);
        EXPECT_EQUAL(transaction->fullResponse.content, "ok");
        EXPECT_EQUAL(transaction->reconnectAttempts, 1);
        EXPECT_EQUAL(server->connections, 1);
    }

    void proxyRefusedReconnectTest()
    {
        // A proxy that refuses every tunnel. Each refusal is retried, and the proxy's own error page is never passed
        // off as the target host's response.
        FailureStageHTTPSManager manager;
        const uint16_t port = FakeHTTPServer::unusedPort();
        FakeHTTPServer proxy(port);

        // This is what `_httpsSend` sets up when proxying, without needing the proxy address it reads from the config.
        SStandaloneHTTPSManager::Transaction transaction(manager, "proxyRefusedReconnectTest");
        transaction.targetHost = "example.com:443";
        transaction.proxyHost = "127.0.0.1:" + to_string(port);
        transaction.isHttps = true;
        transaction.maxReconnectAttempts = SStandaloneHTTPSManager::MAX_RECONNECT_ATTEMPTS;
        transaction.fullRequest = SData("GET / HTTP/1.1");
        transaction.fullRequest["Host"] = "example.com";
        transaction.timeoutAt = STimeNow() + 10'000'000;
        transaction.s = new SHTTPSProxySocket(transaction.proxyHost, transaction.targetHost, transaction.requestID);
        transaction.s->send(transaction.fullRequest.serialize());

        waitForResponse(manager, transaction, 300'000, [&]() {
            proxy.answer("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 11\r\n\r\nproxy error");
        });

        EXPECT_EQUAL(transaction.response, 500);
        EXPECT_FALSE(SContains(transaction.fullResponse.content, "proxy error"));
        EXPECT_EQUAL(transaction.reconnectAttempts, SStandaloneHTTPSManager::MAX_RECONNECT_ATTEMPTS);
        EXPECT_EQUAL(proxy.connections, SStandaloneHTTPSManager::MAX_RECONNECT_ATTEMPTS + 1);
    }

    void noReconnectAfterSendTest()
    {
        // The server accepts the request but never answers. The request reached it, so it must not be sent again.
        FailureStageHTTPSManager manager;
        const uint16_t port = FakeHTTPServer::unusedPort();
        FakeHTTPServer server(port);
        SData request("GET / HTTP/1.1");
        request["Host"] = "127.0.0.1:" + to_string(port);
        unique_ptr<SStandaloneHTTPSManager::Transaction> transaction = manager.send("http://127.0.0.1:" + to_string(port) + "/", request);
        transaction->timeoutAt = STimeNow() + 10'000'000;

        waitForResponse(manager, *transaction, 1'000, [&]() {
            server.answer("");
        });

        EXPECT_EQUAL(transaction->response, 501);
        EXPECT_EQUAL(transaction->reconnectAttempts, 0);
        EXPECT_EQUAL(server.connections, 1);
    }

    void reconnectOptOutTest()
    {
        // A caller that passes 0 gets the connection failure straight away, without a reconnect.
        FailureStageHTTPSManager manager;
        const uint16_t port = FakeHTTPServer::unusedPort();
        SData request("GET / HTTP/1.1");
        request["Host"] = "127.0.0.1:" + to_string(port);
        unique_ptr<SStandaloneHTTPSManager::Transaction> transaction = manager.send("http://127.0.0.1:" + to_string(port) + "/", request, 0);
        transaction->timeoutAt = STimeNow() + 10'000'000;

        waitForResponse(manager, *transaction);

        EXPECT_EQUAL(transaction->response, 500);
        EXPECT_EQUAL(transaction->reconnectAttempts, 0);
        EXPECT_FALSE(transaction->reconnectAt);
    }

    void reconnectAfterTimeoutTest()
    {
        // The reconnect is due, but polling resumes after the transaction's deadline. The request must not be sent.
        FailureStageHTTPSManager manager;
        const uint16_t port = FakeHTTPServer::unusedPort();
        FakeHTTPServer server(port);
        SStandaloneHTTPSManager::Transaction transaction(manager, "reconnectAfterTimeoutTest");
        transaction.targetHost = "127.0.0.1:" + to_string(port);
        transaction.maxReconnectAttempts = SStandaloneHTTPSManager::MAX_RECONNECT_ATTEMPTS;
        transaction.fullRequest = SData("GET / HTTP/1.1");
        transaction.fullRequest["Host"] = transaction.targetHost;
        transaction.reconnectAt = STimeNow() - 2;
        transaction.timeoutAt = STimeNow() - 1;

        waitForResponse(manager, transaction, 1'000, [&]() {
            server.answer("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
        });

        EXPECT_EQUAL(transaction.response, 500);
        EXPECT_EQUAL(transaction.reconnectAttempts, 0);
        EXPECT_FALSE(transaction.s);
        EXPECT_EQUAL(server.connections, 0);
    }
} __SSLTest;
