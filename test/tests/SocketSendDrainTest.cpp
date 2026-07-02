#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/STCPManager.h>
#include <test/lib/BedrockTester.h>

// Regression test for a truncation bug in BedrockServer::_reply(): a single socket->send() call can queue
// fewer bytes than the full response, and the old code shut the socket down immediately afterward
// regardless, silently dropping the rest. This forces that condition deterministically by interrupting a
// blocking send() with a signal partway through a large payload.
struct SocketSendDrainTest : tpunit::TestFixture
{
    SocketSendDrainTest()
        : tpunit::TestFixture("SocketSendDrain", TEST(SocketSendDrainTest::testLargeResponseSurvivesInterruptedSend))
    {
    }

    static const size_t PAYLOAD_SIZE = 20 * 1024 * 1024;
    static const int SOCKET_BUFFER_SIZE = 8 * 1024;

    // No-op: we only need the signal to interrupt the blocking send() below, nothing more.
    static void noopSignalHandler(int)
    {
    }

    // Mirrors the fixed _reply(): loop send() until sendBuffer is drained before shutting down.
    static void sendAndDrainThenShutdown(STCPManager::Socket& socket, const string& data)
    {
        bool sendSucceeded = socket.send(data);
        while (sendSucceeded && !socket.sendBufferEmpty()) {
            sendSucceeded = socket.send();
        }
        socket.shutdown();
    }

    void testLargeResponseSurvivesInterruptedSend()
    {
        // Large enough that a signal has time to land mid-send; distinct bytes make truncation obvious.
        string payload;
        payload.reserve(PAYLOAD_SIZE);
        for (size_t i = 0; i < PAYLOAD_SIZE; i++) {
            payload.push_back(static_cast<char>('A' + (i % 26)));
        }

        // Ignore SIGPIPE so a half-closed socket doesn't kill the test; SIGALRM without SA_RESTART lets us
        // interrupt the blocking send() below.
        signal(SIGPIPE, SIG_IGN);
        struct sigaction sa {};
        sa.sa_handler = noopSignalHandler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        sigaction(SIGALRM, &sa, nullptr);

        int listenFD = socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_TRUE(listenFD >= 0);
        int reuse = 1;
        setsockopt(listenFD, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in listenAddr{};
        listenAddr.sin_family = AF_INET;
        listenAddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        listenAddr.sin_port = 0;
        ASSERT_EQUAL(::bind(listenFD, reinterpret_cast<sockaddr*>(&listenAddr), sizeof(listenAddr)), 0);
        socklen_t addrLen = sizeof(listenAddr);
        ASSERT_EQUAL(getsockname(listenFD, reinterpret_cast<sockaddr*>(&listenAddr), &addrLen), 0);
        ASSERT_EQUAL(listen(listenFD, 1), 0);
        int port = ntohs(listenAddr.sin_port);

        atomic<size_t> bytesReceived(0);
        atomic<bool> receivedPrefixMatches(true);

        // Reads slowly at first to keep the server's send() blocked long enough for the signal to land,
        // then drains as fast as possible.
        thread client([port, &bytesReceived, &receivedPrefixMatches, &payload]() {
            int fd = socket(AF_INET, SOCK_STREAM, 0);
            int rcvBuf = SOCKET_BUFFER_SIZE;
            setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvBuf, sizeof(rcvBuf));
            sockaddr_in serverAddr{};
            serverAddr.sin_family = AF_INET;
            serverAddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            serverAddr.sin_port = htons(port);
            if (connect(fd, reinterpret_cast<sockaddr*>(&serverAddr), sizeof(serverAddr)) != 0) {
                close(fd);
                return;
            }

            string received;
            received.reserve(payload.size());
            char buf[4096];
            auto start = chrono::steady_clock::now();
            for (;;) {
                if (chrono::steady_clock::now() - start < chrono::milliseconds(200)) {
                    this_thread::sleep_for(chrono::milliseconds(2));
                }
                ssize_t n = recv(fd, buf, sizeof(buf), 0);
                if (n <= 0) {
                    break;
                }
                received.append(buf, static_cast<size_t>(n));
            }
            bytesReceived = received.size();
            receivedPrefixMatches = (received == payload.substr(0, received.size()));
            close(fd);
        });

        int acceptedFD = accept(listenFD, nullptr, nullptr);
        ASSERT_TRUE(acceptedFD >= 0);
        int sndBuf = SOCKET_BUFFER_SIZE;
        setsockopt(acceptedFD, SOL_SOCKET, SO_SNDBUF, &sndBuf, sizeof(sndBuf));

        // The same Socket class BedrockServer::_reply() uses -- not a reimplementation.
        STCPManager::Socket serverSocket(acceptedFD, STCPManager::Socket::CONNECTED);

        // pthread_kill targets this specific thread; a process-wide timer signal isn't guaranteed to.
        pthread_t sendingThread = pthread_self();
        atomic<bool> stopInterrupter(false);
        thread interrupter([&sendingThread, &stopInterrupter]() {
            this_thread::sleep_for(chrono::milliseconds(50));
            if (!stopInterrupter) {
                pthread_kill(sendingThread, SIGALRM);
            }
        });

        sendAndDrainThenShutdown(serverSocket, payload);

        stopInterrupter = true;
        interrupter.join();
        close(listenFD);
        client.join();

        // Must receive the complete response, not just whatever was queued before send() was interrupted.
        ASSERT_EQUAL(bytesReceived.load(), payload.size());
        ASSERT_TRUE(receivedPrefixMatches.load());
    }
} __SocketSendDrainTest;
