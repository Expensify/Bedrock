#include <BedrockServer.h>
#include <libstuff/JSON/Value.h>
#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <future>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

struct CommandPortTest : tpunit::TestFixture
{
    CommandPortTest()
        : tpunit::TestFixture("CommandPort",
                              TEST(CommandPortTest::test),
                              TEST(CommandPortTest::testCompleteReply),
                              TEST(CommandPortTest::testPartialReplyBeforeClose),
                              TEST(CommandPortTest::testPartialReplyWithKeepAlive),
                              TEST(CommandPortTest::testMixedCaseConnectionClose),
                              TEST(CommandPortTest::testReplyAfterTemporaryNoProgress),
                              TEST(CommandPortTest::testFailedReplyAfterPartialWrite),
                              TEST(CommandPortTest::testFailedReplyAfterClientReset),
                              TEST(CommandPortTest::testNonBlockingFlushLeavesRemainder),
                              TEST(CommandPortTest::testResponseHandoffRetainsCommand),
                              TEST(CommandPortTest::testShutdownDeadlineStopsDelivery),
                              TEST(CommandPortTest::testRunningStateIgnoresExpiredShutdownDeadline),
                              TEST(CommandPortTest::testReadHalfCloseAllowsDelivery),
                              TEST(CommandPortTest::testSlowReaderDoesNotBlockSynchronousCommands))
    {
    }

    class PartialSendSocket : public STCPManager::Socket {
public:
        PartialSendSocket(int fd, size_t firstWriteLimit, bool pauseAfterFirstWrite, bool failAfterFirstWrite)
            : Socket(fd, CONNECTED), firstWriteLimit(firstWriteLimit), pauseAfterFirstWrite(pauseAfterFirstWrite),
            failAfterFirstWrite(failAfterFirstWrite)
        {
        }

        bool sendNonBlocking() override
        {
            lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);
            sendCalls++;

            // Inject a successful short write without depending on kernel buffer sizes or signal delivery.
            if (sendCalls == 1) {
                const size_t size = min(firstWriteLimit, sendBuffer.size());
                const ssize_t sent = ::send(s, sendBuffer.c_str(), size, MSG_NOSIGNAL | MSG_DONTWAIT);
                if (sent < 0) {
                    return false;
                }
                sendBuffer.consumeFront(static_cast<size_t>(sent));
                return true;
            }

            if (failAfterFirstWrite) {
                return false;
            }

            // Socket::send() also reports true for recoverable errors that leave the entire buffer pending.
            if (sendCalls == 2 && pauseAfterFirstWrite) {
                return true;
            }

            return Socket::sendNonBlocking();
        }

        size_t sendCalls = 0;
        const size_t firstWriteLimit;
        const bool pauseAfterFirstWrite;
        const bool failAfterFirstWrite;
    };

    class ReplyCommand : public BedrockCommand {
public:
        ReplyCommand(SData&& request, size_t& failedReplies)
            : BedrockCommand(SQLiteCommand(move(request)), nullptr), failedReplies(failedReplies)
        {
        }

        void handleFailedReply() override
        {
            failedReplies++;
        }

        ~ReplyCommand() override
        {
            if (onDestroy) {
                onDestroy();
            }
        }

        bool shouldSuppressTimeoutWarnings() override
        {
            return true;
        }

        function<void()> onDestroy;
        size_t& failedReplies;
    };

    void testCompleteReply()
    {
        verifyReply("close", 16384, false);
    }

    void testPartialReplyBeforeClose()
    {
        verifyReply("close", 64, false);
    }

    void testPartialReplyWithKeepAlive()
    {
        verifyReply("keep-alive", 64, false);
    }

    void testMixedCaseConnectionClose()
    {
        verifyReply("Close", 64, false);
    }

    void testRunningStateIgnoresExpiredShutdownDeadline()
    {
        verifyReply("close", 16384, false, false, chrono::steady_clock::now() - chrono::seconds(1));
    }

    void testReplyAfterTemporaryNoProgress()
    {
        verifyReply("close", 64, true);
    }

    void testFailedReplyAfterPartialWrite()
    {
        verifyReply("close", 64, false, true);
    }

    void testFailedReplyAfterClientReset()
    {
        STCPManager::Socket listener(::socket(AF_INET, SOCK_STREAM, 0), STCPManager::Socket::CONNECTED);
        ASSERT_TRUE(listener.s >= 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ASSERT_EQUAL(bind(listener.s, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
        socklen_t addressSize = sizeof(address);
        ASSERT_EQUAL(getsockname(listener.s, reinterpret_cast<sockaddr*>(&address), &addressSize), 0);
        ASSERT_EQUAL(listen(listener.s, 1), 0);

        // Small buffers and no client reads keep the multi-megabyte blocking write in progress.
        STCPManager::Socket receiver(::socket(AF_INET, SOCK_STREAM, 0), STCPManager::Socket::CONNECTED);
        ASSERT_TRUE(receiver.s >= 0);
        const int bufferSize = 4096;
        ASSERT_EQUAL(setsockopt(receiver.s, SOL_SOCKET, SO_RCVBUF, &bufferSize, sizeof(bufferSize)), 0);
        ASSERT_EQUAL(connect(receiver.s, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
        STCPManager::Socket sender(accept(listener.s, nullptr, nullptr), STCPManager::Socket::CONNECTED);
        ASSERT_TRUE(sender.s >= 0);
        ASSERT_EQUAL(setsockopt(sender.s, SOL_SOCKET, SO_SNDBUF, &bufferSize, sizeof(bufferSize)), 0);

        SData args;
        args["-nodeName"] = "reset-reply-test";
        BedrockServer server(SQLiteNodeState::LEADING, args);
        server._shutdownState = BedrockServer::RUNNING;
        server._outstandingSocketThreads = 0;
        SData request("Query");
        request["Connection"] = "close";
        size_t failedReplies = 0;
        unique_ptr<BedrockCommand> command = make_unique<ReplyCommand>(move(request), failedReplies);
        command->socket = &sender;
        command->initiatingClientID = static_cast<int64_t>(sender.id);
        command->response.methodLine = "200 OK";
        command->response.content = string(4 * 1024 * 1024, 'x');
        server._reply(command);

        thread sendingThread([&]() {
            SInitialize("reset-reply-test");
            server._deliverReply(command);
        });

        // Read readiness proves some response bytes have arrived. Reset without consuming them, so the kernel
        // interrupts the in-progress write. Close and join before asserting, including on a setup timeout.
        pollfd readable = {receiver.s, POLLIN, 0};
        const int ready = poll(&readable, 1, 5000);
        const linger reset = {1, 0};
        const int lingerResult = setsockopt(receiver.s, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
        const int closeResult = close(receiver.s);
        receiver.s = -1;
        sendingThread.join();

        ASSERT_EQUAL(ready, 1);
        ASSERT_TRUE(readable.revents & POLLIN);
        ASSERT_EQUAL(lingerResult, 0);
        ASSERT_EQUAL(closeResult, 0);
        const size_t pending = sender.sendBufferCopy().size();
        const size_t total = command->response.serialize().size();
        cout << "Reset during reply: " << total - pending << "/" << total
        << " bytes accepted, failed-reply callbacks: " << failedReplies << endl;
        ASSERT_TRUE(pending > 0 && pending < total);
        EXPECT_EQUAL(failedReplies, 1U);
    }

    void testNonBlockingFlushLeavesRemainder()
    {
        int fds[2];
        ASSERT_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
        STCPManager::Socket sender(fds[0], STCPManager::Socket::CONNECTED);
        STCPManager::Socket receiver(fds[1], STCPManager::Socket::CONNECTED);
        const int bufferSize = 4096;
        ASSERT_EQUAL(setsockopt(sender.s, SOL_SOCKET, SO_SNDBUF, &bufferSize, sizeof(bufferSize)), 0);
        const int flags = fcntl(sender.s, F_GETFL);
        ASSERT_TRUE(flags >= 0 && !(flags & O_NONBLOCK));
        const size_t size = 4 * 1024 * 1024;
        sender.setSendBuffer(string(size, 'x'));

        promise<bool> result;
        auto completed = result.get_future();
        thread flushing([&]() {
            SInitialize("nonblocking-flush-test");
            result.set_value(sender.sendNonBlocking() && sender.sendNonBlocking());
        });
        const bool ready = completed.wait_for(chrono::seconds(2)) == future_status::ready;
        if (!ready) {
            receiver.shutdown(STCPManager::Socket::CLOSED);
        }
        flushing.join();
        ASSERT_TRUE(ready);
        EXPECT_TRUE(completed.get());
        const size_t pending = sender.sendBufferCopy().size();
        EXPECT_TRUE(pending > 0 && pending < size);
        EXPECT_EQUAL(fcntl(sender.s, F_GETFL), flags);
    }

    void testResponseHandoffRetainsCommand()
    {
        int fds[2];
        ASSERT_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
        PartialSendSocket sender(fds[0], 64, false, true);
        STCPManager::Socket receiver(fds[1], STCPManager::Socket::CONNECTED);
        SData args;
        BedrockServer server(SQLiteNodeState::LEADING, args);
        server._shutdownState = BedrockServer::RUNNING;
        server._outstandingSocketThreads = 0;
        SData request("Query");
        request["timeout"] = "-1";
        size_t failedReplies = 0;
        size_t destroyed = 0;
        size_t destructionNotifications = 0;
        auto observed = make_unique<ReplyCommand>(move(request), failedReplies);
        observed->onDestroy = [&]() {
            destroyed++;
        };
        unique_ptr<BedrockCommand> response;
        observed->responseCallback = [&](unique_ptr<BedrockCommand> ready) {
            response = move(ready);
        };
        function<void()> notification = [&]() {
            destructionNotifications++;
        };
        observed->destructionCallback = &notification;
        observed->socket = &sender;
        observed->initiatingClientID = static_cast<int64_t>(sender.id);
        unique_ptr<BedrockCommand> command = move(observed);

        server.runCommand(move(command));
        ASSERT_TRUE(response);
        EXPECT_FALSE(command);
        EXPECT_TRUE(response->responseReady);
        EXPECT_EQUAL(destroyed, 0U);
        EXPECT_EQUAL(destructionNotifications, 0U);
        EXPECT_EQUAL(sender.sendCalls, 0U);
        EXPECT_TRUE(response->destructionCallback == nullptr);
        EXPECT_FALSE(server._deliverReply(response));
        EXPECT_EQUAL(failedReplies, 1U);
        response.reset();
        EXPECT_EQUAL(destroyed, 1U);
        EXPECT_EQUAL(destructionNotifications, 0U);
    }

    void testShutdownDeadlineStopsDelivery()
    {
        int fds[2];
        ASSERT_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
        PartialSendSocket sender(fds[0], 64, false, false);
        STCPManager::Socket receiver(fds[1], STCPManager::Socket::CONNECTED);
        SData args;
        BedrockServer server(SQLiteNodeState::LEADING, args);
        server._shutdownState = BedrockServer::START_SHUTDOWN;
        server._shutdownTime = chrono::steady_clock::now() - chrono::seconds(1);
        server._outstandingSocketThreads = 0;
        size_t failedReplies = 0;
        unique_ptr<BedrockCommand> command = make_unique<ReplyCommand>(SData("Query"), failedReplies);
        command->socket = &sender;
        command->initiatingClientID = static_cast<int64_t>(sender.id);
        command->response.methodLine = "200 OK";
        server._reply(command);
        EXPECT_FALSE(server._deliverReply(command));
        EXPECT_EQUAL(failedReplies, 1U);
        EXPECT_EQUAL(sender.sendCalls, 0U);
        EXPECT_EQUAL(sender.state.load(), STCPManager::Socket::CLOSED);
    }

    void testSlowReaderDoesNotBlockSynchronousCommands()
    {
        BedrockTester tester({{"-synchronousCommands", "Query"}}, {});
        STCPManager::Socket client(::socket(AF_INET, SOCK_STREAM, 0), STCPManager::Socket::CONNECTED);
        ASSERT_TRUE(client.s >= 0);
        const int bufferSize = 4096;
        ASSERT_EQUAL(setsockopt(client.s, SOL_SOCKET, SO_RCVBUF, &bufferSize, sizeof(bufferSize)), 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        const string host = tester.getArg("-serverHost");
        address.sin_port = htons(static_cast<uint16_t>(SToInt(host.substr(host.rfind(':') + 1))));
        ASSERT_EQUAL(connect(client.s, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
        SData large("Query");
        large["query"] = "SELECT hex(zeroblob(8388608)) AS payload;";
        large["Connection"] = "close";
        ASSERT_TRUE(client.send(large.serialize()));
        ASSERT_TRUE(client.sendBufferEmpty());
        pollfd readable = {client.s, POLLIN, 0};
        ASSERT_EQUAL(poll(&readable, 1, 5000), 1);
        ASSERT_TRUE(readable.revents & POLLIN);

        // Both Queries use the single blocking-commit worker. The second must finish while the first client
        // still isn't reading; releasing the first connection before checking would hide worker-thread writes.
        promise<bool> result;
        auto completed = result.get_future();
        thread anotherCommand([&]() {
            SInitialize("slow-reader-second-query");
            try {
                SData small("Query");
                small["query"] = "SELECT 1 AS ready;";
                tester.executeWaitVerifyContent(small);
                result.set_value(true);
            } catch (...) {
                result.set_exception(current_exception());
            }
        });
        const bool ready = completed.wait_for(chrono::seconds(5)) == future_status::ready;
        const linger reset = {1, 0};
        const int lingerResult = setsockopt(client.s, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
        const int closeResult = close(client.s);
        client.s = -1;
        anotherCommand.join();
        ASSERT_TRUE(ready);
        EXPECT_TRUE(completed.get());
        EXPECT_EQUAL(lingerResult, 0);
        EXPECT_EQUAL(closeResult, 0);
    }

    void testReadHalfCloseAllowsDelivery()
    {
        int fds[2];
        ASSERT_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
        STCPManager::Socket sender(fds[0], STCPManager::Socket::CONNECTED);
        STCPManager::Socket receiver(fds[1], STCPManager::Socket::CONNECTED);
        ASSERT_EQUAL(shutdown(receiver.s, SHUT_WR), 0);
        SData args;
        BedrockServer server(SQLiteNodeState::LEADING, args);
        server._shutdownState = BedrockServer::RUNNING;
        server._outstandingSocketThreads = 0;
        SData request("Query");
        request["Connection"] = "close";
        size_t failedReplies = 0;
        unique_ptr<BedrockCommand> command = make_unique<ReplyCommand>(move(request), failedReplies);
        command->socket = &sender;
        command->initiatingClientID = static_cast<int64_t>(sender.id);
        command->response.methodLine = "200 OK";
        command->response.content = "complete";
        server._reply(command);
        EXPECT_TRUE(server._deliverReply(command));
        char received[1024];
        const ssize_t count = recv(receiver.s, received, sizeof(received), MSG_DONTWAIT);
        ASSERT_TRUE(count > 0);
        EXPECT_EQUAL(string(received, static_cast<size_t>(count)), command->response.serialize());
        EXPECT_EQUAL(failedReplies, 0U);
    }

    void verifyReply(const string& connection, size_t firstWriteLimit, bool pauseAfterFirstWrite,
                     bool failAfterFirstWrite = false, chrono::time_point<chrono::steady_clock> shutdownTime = {})
    {
        int fds[2];
        ASSERT_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
        PartialSendSocket sender(fds[0], firstWriteLimit, pauseAfterFirstWrite, failAfterFirstWrite);
        STCPManager::Socket receiver(fds[1], STCPManager::Socket::CONNECTED);

        // The stub constructor starts no database or worker threads; only the reply path runs here.
        SData args;
        args["-nodeName"] = "reply-test";
        BedrockServer server(SQLiteNodeState::LEADING, args);
        server._shutdownState = BedrockServer::RUNNING;
        server._outstandingSocketThreads = 0;
        server._shutdownTime = shutdownTime;

        SData request("Query");
        request["Connection"] = connection;
        size_t failedReplies = 0;
        unique_ptr<BedrockCommand> command = make_unique<ReplyCommand>(move(request), failedReplies);
        command->socket = &sender;
        command->initiatingClientID = static_cast<int64_t>(sender.id);
        command->response.methodLine = "200 OK";
        command->response.content = string(8192, 'x');

        server._reply(command);
        EXPECT_TRUE(command->responseReady);
        EXPECT_EQUAL(sender.sendCalls, 0U);
        EXPECT_TRUE(sender.sendBufferEmpty());
        EXPECT_EQUAL(sender.state.load(), STCPManager::Socket::CONNECTED);
        const bool delivered = server._deliverReply(command);
        EXPECT_EQUAL(delivered, !failAfterFirstWrite);

        string received;
        char buffer[16384];
        ssize_t count = recv(receiver.s, buffer, sizeof(buffer), MSG_DONTWAIT);
        while (count > 0) {
            received.append(buffer, static_cast<size_t>(count));
            count = recv(receiver.s, buffer, sizeof(buffer), MSG_DONTWAIT);
        }
        ASSERT_TRUE(count == 0 || (count == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)));

        const string expected = command->response.serialize();
        cout << "Reply bytes: " << received.size() << "/" << expected.size()
        << ", pending: " << sender.sendBufferCopy().size() << ", sends: " << sender.sendCalls << endl;
        if (failAfterFirstWrite) {
            EXPECT_EQUAL(received.size(), firstWriteLimit);
            EXPECT_TRUE(received == expected.substr(0, received.size()));
            EXPECT_EQUAL(failedReplies, 1U);
        } else {
            EXPECT_EQUAL(received.size(), expected.size());
            EXPECT_TRUE(received == expected);
            EXPECT_EQUAL(failedReplies, 0U);
        }
        if (SIEquals(connection, "close")) {
            EXPECT_EQUAL(count, 0);
        } else {
            EXPECT_EQUAL(sender.state.load(), STCPManager::Socket::CONNECTED);
        }
    }

    void test()
    {
        BedrockTester tester;

        // When we close the command port with a reason
        SData closeCommandPort("SuppressCommandPort");
        closeCommandPort["reason"] = "testCommandPort";
        tester.executeWaitMultipleData({closeCommandPort});
        JSON::Value response;

        // The status command should show it in commandPortBlockReasons
        SData status("Status");
        response = JSON::Value::parse(tester.executeWaitMultipleData({status}, 10, true)[0].content);
        ASSERT_EQUAL(static_cast<list<string>>(response["commandPortBlockReasons"]), list<string>{"testCommandPort"});

        // When we run ClearCommandPort with a reason different from the one used to close it
        SData badClearCommandPort("ClearCommandPort");
        tester.executeWaitMultipleData({badClearCommandPort}, 10, true);

        // The command port should stay close and the status command should still show the reason the port is closed in commandPortBlockReasons
        response = JSON::Value::parse(tester.executeWaitMultipleData({status}, 10, true)[0].content);
        ASSERT_EQUAL(static_cast<list<string>>(response["commandPortBlockReasons"]), list<string>{"testCommandPort"});

        // When we run ClearCommandPort with the same reason as the one used to close it
        SData clearCommandPort("ClearCommandPort");
        clearCommandPort["reason"] = "testCommandPort";
        tester.executeWaitMultipleData({clearCommandPort}, 10, true);

        // Then the command port should open and the reason should be removed from commandPortBlockReasons
        response = JSON::Value::parse(tester.executeWaitMultipleData({status})[0].content);
        ASSERT_EQUAL(static_cast<list<string>>(response["commandPortBlockReasons"]), list<string>{});

        SData setMaxOutstandingWALFrames("SetMaxOutstandingWALFrames");
        setMaxOutstandingWALFrames["maxOutstandingWALFrames"] = "300000";
        tester.executeWaitMultipleData({setMaxOutstandingWALFrames}, 10, true);
        response = JSON::Value::parse(tester.executeWaitMultipleData({status}, 10, true)[0].content);
        ASSERT_EQUAL(response["maxOutstandingWALFrames"].getUint(), 300000);

        setMaxOutstandingWALFrames["maxOutstandingWALFrames"] = "0";
        tester.executeWaitMultipleData({setMaxOutstandingWALFrames}, 10, true);
        response = JSON::Value::parse(tester.executeWaitMultipleData({status}, 10, true)[0].content);
        ASSERT_EQUAL(response["maxOutstandingWALFrames"].getUint(), 0);
    }
} __CommandPortTest;
