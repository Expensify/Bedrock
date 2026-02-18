#include <thread>
#include <chrono>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <atomic>
#include <syslog.h>

#include <libstuff/SFluentdLogger.h>
#include <test/lib/BedrockTester.h>

// Mock TCP server that counts newline-delimited messages
class MockServer {
public:
    int port = 0;
    atomic<int> messageCount{0};

    MockServer()
    {
        // Create socket
        serverFd = socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        // Bind to random port
        struct sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(0);
        bind(serverFd, (struct sockaddr*) &addr, sizeof(addr));
        listen(serverFd, 1);

        // Get assigned port
        socklen_t len = sizeof(addr);
        getsockname(serverFd, (struct sockaddr*) &addr, &len);
        port = ntohs(addr.sin_port);

        // Accept and count messages in background
        acceptThread = thread([this]() {
            int clientFd = accept(serverFd, nullptr, nullptr);
            if (clientFd >= 0) {
                char buf[4096];
                ssize_t n;
                while ((n = recv(clientFd, buf, sizeof(buf), 0)) > 0) {
                    for (ssize_t i = 0; i < n; i++) {
                        if (buf[i] == '\n') {
                            messageCount++;
                        }
                    }
                }
                close(clientFd);
            }
        });
    }

    ~MockServer()
    {
        close(serverFd);
        acceptThread.join();
    }

private:
    int serverFd;
    thread acceptThread;
};

struct SFluentdLoggerTest : tpunit::TestFixture
{
    SFluentdLoggerTest() : tpunit::TestFixture(
        "SFluentdLogger",
        TEST(SFluentdLoggerTest::testBuffersWithoutServer),
        TEST(SFluentdLoggerTest::testMultipleMessages),
        TEST(SFluentdLoggerTest::testDrainOnShutdown)
    )
    {
    }

    // Test log() buffers messages even without a server
    void testBuffersWithoutServer()
    {
        // Create logger pointing to non-existent server
        SFluentdLogger logger("127.0.0.1", 59999);

        // All logs return true (buffered)
        for (int i = 0; i < 100; i++) {
            string json = "{\"count\":" + to_string(i) + "}\n";
            ASSERT_TRUE(logger.log(LOG_INFO, move(json)));
        }
    }

    // Test multiple messages are sent to server
    void testMultipleMessages()
    {
        // Start mock server
        MockServer server;

        {
            // Create logger and send 50 messages
            SFluentdLogger logger("127.0.0.1", server.port);
            for (int i = 0; i < 50; i++) {
                string json = "{\"msg\":" + to_string(i) + "}\n";
                logger.log(LOG_INFO, move(json));
            }

            // Wait for sender thread to transmit
            this_thread::sleep_for(chrono::milliseconds(100));
        }

        // All messages received
        ASSERT_EQUAL(server.messageCount.load(), 50);
    }

    // Test destructor drains all buffered messages
    void testDrainOnShutdown()
    {
        // Start mock server
        MockServer server;

        {
            // Create logger and send 100 messages
            SFluentdLogger logger("127.0.0.1", server.port);
            for (int i = 0; i < 100; i++) {
                string json = "{\"drain\":" + to_string(i) + "}\n";
                logger.log(move(json));
            }

            // Destructor drains buffer
        }

        // Wait for server to finish receiving
        this_thread::sleep_for(chrono::milliseconds(50));

        // All messages drained before shutdown
        ASSERT_EQUAL(server.messageCount.load(), 100);
    }
} __SFluentdLoggerTest;
