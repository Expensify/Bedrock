#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#include <libstuff/libstuff.h>
#include <libstuff/SHTTPSManager.h>
#include <test/lib/BedrockTester.h>

struct SHTTPSManagerStreamTest : tpunit::TestFixture
{
    SHTTPSManagerStreamTest() : tpunit::TestFixture(
        "SHTTPSManagerStream",
        TEST(SHTTPSManagerStreamTest::testNonStreamingUnaffected),
        TEST(SHTTPSManagerStreamTest::testBasicSSEParsing),
        TEST(SHTTPSManagerStreamTest::testMultiEventBuffering),
        TEST(SHTTPSManagerStreamTest::testPartialEventAcrossReads),
        TEST(SHTTPSManagerStreamTest::testDoneTermination),
        TEST(SHTTPSManagerStreamTest::testHeadersAndBodyInSameRead),
        TEST(SHTTPSManagerStreamTest::testErrorResponse),
        TEST(SHTTPSManagerStreamTest::testSocketClose)
    )
    {
    }

    // Runs one prePoll -> S_poll -> pollStreamingData cycle.
    bool pollOnce(SStandaloneHTTPSManager& manager, SStandaloneHTTPSManager::Transaction& transaction, uint64_t& nextActivity)
    {
        fd_map fdm;
        manager.prePoll(fdm, transaction);
        S_poll(fdm, 10 * 1000);
        return manager.pollStreamingData(fdm, transaction, nextActivity);
    }

    // Runs one prePoll -> S_poll -> postPoll cycle (non-streaming).
    void postPollOnce(SStandaloneHTTPSManager& manager, SStandaloneHTTPSManager::Transaction& transaction, uint64_t& nextActivity)
    {
        fd_map fdm;
        manager.prePoll(fdm, transaction);
        S_poll(fdm, 10 * 1000);
        manager.postPoll(fdm, transaction, nextActivity);
    }

    void writeToSocket(int fd, const string& data)
    {
        size_t written = 0;
        while (written < data.size()) {
            ssize_t n = ::write(fd, data.c_str() + written, data.size() - written);
            ASSERT_TRUE(n > 0);
            written += n;
        }
    }

    // Verifies that the existing postPoll path still works correctly for non-streaming transactions.
    void testNonStreamingUnaffected()
    {
        int fds[2];
        ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

        SStandaloneHTTPSManager manager;
        auto transaction = make_unique<SStandaloneHTTPSManager::Transaction>(manager);
        transaction->s = new STCPManager::Socket(fds[0], STCPManager::Socket::CONNECTED);
        transaction->timeoutAt = STimeNow() + 30 * 1000 * 1000;

        string response = "HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\nHello, world!";
        writeToSocket(fds[1], response);

        uint64_t nextActivity = STimeNow() + 60 * 1000 * 1000;
        for (int i = 0; i < 10 && !transaction->finished; i++) {
            postPollOnce(manager, *transaction, nextActivity);
        }

        ASSERT_TRUE(transaction->finished);
        ASSERT_EQUAL(transaction->response, 200);
        ASSERT_EQUAL(transaction->fullResponse.content, "Hello, world!");
        close(fds[1]);
    }

    void testBasicSSEParsing()
    {
        int fds[2];
        ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

        SStandaloneHTTPSManager manager;
        auto transaction = make_unique<SStandaloneHTTPSManager::Transaction>(manager);
        transaction->s = new STCPManager::Socket(fds[0], STCPManager::Socket::CONNECTED);
        transaction->timeoutAt = STimeNow() + 30 * 1000 * 1000;

        vector<string> receivedEvents;
        transaction->streamCallback = [&receivedEvents](const string& data) {
            receivedEvents.push_back(data);
        };

        // Send headers.
        writeToSocket(fds[1], "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n");

        uint64_t nextActivity = STimeNow() + 60 * 1000 * 1000;
        bool done = pollOnce(manager, *transaction, nextActivity);
        ASSERT_FALSE(done);
        ASSERT_TRUE(transaction->streamHeadersParsed);
        ASSERT_EQUAL(transaction->response, 200);
        ASSERT_EQUAL(receivedEvents.size(), 0);

        // Send one event.
        writeToSocket(fds[1], "data: {\"text\":\"hello\"}\n\n");

        done = pollOnce(manager, *transaction, nextActivity);
        ASSERT_FALSE(done);
        ASSERT_EQUAL(receivedEvents.size(), 1);
        ASSERT_EQUAL(receivedEvents[0], "data: {\"text\":\"hello\"}");

        // Send termination.
        writeToSocket(fds[1], "data: [DONE]\n\n");

        done = pollOnce(manager, *transaction, nextActivity);
        ASSERT_TRUE(done);
        ASSERT_TRUE(transaction->finished);
        ASSERT_EQUAL(receivedEvents.size(), 1);

        close(fds[1]);
    }

    // Multiple complete events arrive in a single read.
    void testMultiEventBuffering()
    {
        int fds[2];
        ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

        SStandaloneHTTPSManager manager;
        auto transaction = make_unique<SStandaloneHTTPSManager::Transaction>(manager);
        transaction->s = new STCPManager::Socket(fds[0], STCPManager::Socket::CONNECTED);
        transaction->timeoutAt = STimeNow() + 30 * 1000 * 1000;

        vector<string> receivedEvents;
        transaction->streamCallback = [&receivedEvents](const string& data) {
            receivedEvents.push_back(data);
        };

        writeToSocket(fds[1], "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n");

        uint64_t nextActivity = STimeNow() + 60 * 1000 * 1000;
        pollOnce(manager, *transaction, nextActivity);

        // Send three events in one write.
        writeToSocket(fds[1],
            "data: {\"n\":1}\n\n"
            "data: {\"n\":2}\n\n"
            "data: {\"n\":3}\n\n"
        );

        pollOnce(manager, *transaction, nextActivity);
        ASSERT_EQUAL(receivedEvents.size(), 3);
        ASSERT_EQUAL(receivedEvents[0], "data: {\"n\":1}");
        ASSERT_EQUAL(receivedEvents[1], "data: {\"n\":2}");
        ASSERT_EQUAL(receivedEvents[2], "data: {\"n\":3}");

        writeToSocket(fds[1], "data: [DONE]\n\n");
        bool done = pollOnce(manager, *transaction, nextActivity);
        ASSERT_TRUE(done);

        close(fds[1]);
    }

    // An event is split across two reads.
    void testPartialEventAcrossReads()
    {
        int fds[2];
        ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

        SStandaloneHTTPSManager manager;
        auto transaction = make_unique<SStandaloneHTTPSManager::Transaction>(manager);
        transaction->s = new STCPManager::Socket(fds[0], STCPManager::Socket::CONNECTED);
        transaction->timeoutAt = STimeNow() + 30 * 1000 * 1000;

        vector<string> receivedEvents;
        transaction->streamCallback = [&receivedEvents](const string& data) {
            receivedEvents.push_back(data);
        };

        writeToSocket(fds[1], "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n");

        uint64_t nextActivity = STimeNow() + 60 * 1000 * 1000;
        pollOnce(manager, *transaction, nextActivity);

        // Send first half of an event.
        writeToSocket(fds[1], "data: {\"partial\":");

        bool done = pollOnce(manager, *transaction, nextActivity);
        ASSERT_FALSE(done);
        ASSERT_EQUAL(receivedEvents.size(), 0);

        // Send second half + terminator.
        writeToSocket(fds[1], "true}\n\ndata: [DONE]\n\n");

        done = pollOnce(manager, *transaction, nextActivity);
        ASSERT_TRUE(done);
        ASSERT_EQUAL(receivedEvents.size(), 1);
        ASSERT_EQUAL(receivedEvents[0], "data: {\"partial\":true}");

        close(fds[1]);
    }

    // data: [DONE] terminates the stream and no more events are dispatched.
    void testDoneTermination()
    {
        int fds[2];
        ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

        SStandaloneHTTPSManager manager;
        auto transaction = make_unique<SStandaloneHTTPSManager::Transaction>(manager);
        transaction->s = new STCPManager::Socket(fds[0], STCPManager::Socket::CONNECTED);
        transaction->timeoutAt = STimeNow() + 30 * 1000 * 1000;

        vector<string> receivedEvents;
        transaction->streamCallback = [&receivedEvents](const string& data) {
            receivedEvents.push_back(data);
        };

        writeToSocket(fds[1], "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n");

        uint64_t nextActivity = STimeNow() + 60 * 1000 * 1000;
        pollOnce(manager, *transaction, nextActivity);

        // [DONE] after one event; trailing data after [DONE] should be ignored.
        writeToSocket(fds[1], "data: {\"n\":1}\n\ndata: [DONE]\n\ndata: {\"n\":2}\n\n");

        bool done = pollOnce(manager, *transaction, nextActivity);
        ASSERT_TRUE(done);
        ASSERT_EQUAL(receivedEvents.size(), 1);
        ASSERT_EQUAL(receivedEvents[0], "data: {\"n\":1}");

        close(fds[1]);
    }

    // Headers and body data arrive together in the first read.
    void testHeadersAndBodyInSameRead()
    {
        int fds[2];
        ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

        SStandaloneHTTPSManager manager;
        auto transaction = make_unique<SStandaloneHTTPSManager::Transaction>(manager);
        transaction->s = new STCPManager::Socket(fds[0], STCPManager::Socket::CONNECTED);
        transaction->timeoutAt = STimeNow() + 30 * 1000 * 1000;

        vector<string> receivedEvents;
        transaction->streamCallback = [&receivedEvents](const string& data) {
            receivedEvents.push_back(data);
        };

        // Headers + first event + DONE all in one write.
        writeToSocket(fds[1],
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n"
            "data: {\"n\":1}\n\n"
            "data: [DONE]\n\n"
        );

        uint64_t nextActivity = STimeNow() + 60 * 1000 * 1000;
        bool done = pollOnce(manager, *transaction, nextActivity);
        ASSERT_TRUE(done);
        ASSERT_TRUE(transaction->streamHeadersParsed);
        ASSERT_EQUAL(receivedEvents.size(), 1);
        ASSERT_EQUAL(receivedEvents[0], "data: {\"n\":1}");

        close(fds[1]);
    }

    // Error HTTP status finishes the stream immediately.
    void testErrorResponse()
    {
        int fds[2];
        ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

        SStandaloneHTTPSManager manager;
        auto transaction = make_unique<SStandaloneHTTPSManager::Transaction>(manager);
        transaction->s = new STCPManager::Socket(fds[0], STCPManager::Socket::CONNECTED);
        transaction->timeoutAt = STimeNow() + 30 * 1000 * 1000;

        vector<string> receivedEvents;
        transaction->streamCallback = [&receivedEvents](const string& data) {
            receivedEvents.push_back(data);
        };

        writeToSocket(fds[1], "HTTP/1.1 429 Too Many Requests\r\nContent-Type: text/plain\r\n\r\nRate limited");

        uint64_t nextActivity = STimeNow() + 60 * 1000 * 1000;
        bool done = pollOnce(manager, *transaction, nextActivity);
        ASSERT_TRUE(done);
        ASSERT_TRUE(transaction->finished);
        ASSERT_EQUAL(transaction->response, 429);
        ASSERT_EQUAL(receivedEvents.size(), 0);

        close(fds[1]);
    }

    // Remote closing the socket finishes the stream.
    void testSocketClose()
    {
        int fds[2];
        ASSERT_TRUE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

        SStandaloneHTTPSManager manager;
        auto transaction = make_unique<SStandaloneHTTPSManager::Transaction>(manager);
        transaction->s = new STCPManager::Socket(fds[0], STCPManager::Socket::CONNECTED);
        transaction->timeoutAt = STimeNow() + 30 * 1000 * 1000;

        vector<string> receivedEvents;
        transaction->streamCallback = [&receivedEvents](const string& data) {
            receivedEvents.push_back(data);
        };

        writeToSocket(fds[1],
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n"
            "data: {\"n\":1}\n\n"
        );

        uint64_t nextActivity = STimeNow() + 60 * 1000 * 1000;
        bool done = pollOnce(manager, *transaction, nextActivity);
        ASSERT_FALSE(done);
        ASSERT_EQUAL(receivedEvents.size(), 1);

        // Close the write end to simulate remote disconnect.
        close(fds[1]);

        // May need a couple poll cycles for the socket to detect the close.
        for (int i = 0; i < 5 && !done; i++) {
            done = pollOnce(manager, *transaction, nextActivity);
        }
        ASSERT_TRUE(done);
        ASSERT_TRUE(transaction->finished);
    }

} __SHTTPSManagerStreamTest;
