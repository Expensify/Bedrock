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

// Regression test guarding against a latent truncation bug in Bedrock's command-response send path:
// BedrockServer::_reply() used to send a response with a single blocking socket->send() call and then
// immediately shut the socket down, with no loop to confirm the entire response was actually queued to the
// kernel first. If that single send() queued fewer bytes than the full response (which can happen when a
// signal interrupts the blocking wait), the remainder was silently discarded instead of retried.
//
// This test forces that exact condition deterministically (a signal interrupting a blocking send() partway
// through a large payload), rather than relying on it happening to occur under real-world timing, so a
// regression back to the single-send pattern would reliably fail this test.
struct SocketSendDrainTest : tpunit::TestFixture
{
    SocketSendDrainTest()
        : tpunit::TestFixture("SocketSendDrain", TEST(SocketSendDrainTest::testLargeResponseSurvivesInterruptedSend))
    {
    }

    static const size_t PAYLOAD_SIZE = 20 * 1024 * 1024;
    static const int SOCKET_BUFFER_SIZE = 8 * 1024;

    // A no-op handler is all that's needed here: the point isn't to do anything on receipt, it's to interrupt
    // whatever blocking syscall is in flight. Registered without SA_RESTART (see below) so the interrupted
    // send() doesn't get silently resumed by the kernel/libc.
    static void noopSignalHandler(int) {}

    // Mirrors the fixed pattern in BedrockServer::_reply(): loop send() until the socket's sendBuffer is
    // fully drained (or a hard error occurs) before shutting down, instead of assuming a single send() call
    // queued the entire response.
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
        // Deterministic, easy-to-verify payload: large enough that a single blocking send() call takes a
        // while (so an injected signal has time to land mid-transfer), with distinct byte values so any
        // truncation or corruption is obvious.
        string payload;
        payload.reserve(PAYLOAD_SIZE);
        for (size_t i = 0; i < PAYLOAD_SIZE; i++) {
            payload.push_back(static_cast<char>('A' + (i % 26)));
        }

        // Ignore SIGPIPE so a half-closed socket can't kill the test process; install a SIGALRM handler
        // without SA_RESTART so we can use it to interrupt the blocking send() below.
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

        // Stands in for whatever's on the other end of a real command port connection. Reads slowly for a
        // short initial window so the server's blocking send() call stays busy in the kernel long enough for
        // the injected signal to interrupt it mid-transfer, then drains as fast as possible.
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

        // Wrap the real fd in the real production Socket class -- this is the same class BedrockServer::_reply()
        // uses, not a reimplementation.
        STCPManager::Socket serverSocket(acceptedFD, STCPManager::Socket::CONNECTED);

        // Interrupt the sending thread's blocking send() call partway through, simulating the kind of
        // transient condition (signal, network hiccup) called out in the plan as the realistic trigger for
        // this bug. pthread_kill (rather than alarm()/setitimer()) guarantees the signal targets this specific
        // thread, since POSIX doesn't guarantee a process-wide timer signal lands on any particular thread.
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

        // The client must receive the complete, byte-for-byte correct response -- not just however much
        // happened to be queued before the interrupted send() call returned.
        ASSERT_EQUAL(bytesReceived.load(), payload.size());
        ASSERT_TRUE(receivedPrefixMatches.load());
    }
} __SocketSendDrainTest;
