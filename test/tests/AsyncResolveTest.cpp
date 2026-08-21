#include <arpa/inet.h>

#include <libstuff/libstuff.h>
#include <libstuff/SResolver.h>
#include <libstuff/STCPManager.h>
#include <test/lib/BedrockTester.h>

struct AsyncResolve : tpunit::TestFixture
{
    AsyncResolve() : tpunit::TestFixture(true, "AsyncResolve",
                                         TEST(AsyncResolve::testRawIPNeverDefers),
                                         TEST(AsyncResolve::testCachedHostNeverDefers),
                                         TEST(AsyncResolve::testDeferredConnectCompletes),
                                         TEST(AsyncResolve::testFailedResolutionClosesSocket),
                                         TEST(AsyncResolve::testAbandonWhileResolving),
                                         TEST(AsyncResolve::testResolverOutlivesCaller))
    {
    }

    // Drives a socket through prePoll/postPoll until it leaves the state it started in, or until
    // we give up. Returns the state it landed in.
    STCPManager::Socket::State pollUntilSettled(STCPManager::Socket& socket, uint64_t timeoutUS = 10'000'000)
    {
        const uint64_t giveUpAt = STimeNow() + timeoutUS;
        while (STimeNow() < giveUpAt) {
            const STCPManager::Socket::State state = socket.state.load();
            if (state != STCPManager::Socket::RESOLVING && state != STCPManager::Socket::CONNECTING) {
                return state;
            }
            fd_map fdm;
            STCPManager::prePoll(fdm, socket);
            S_poll(fdm, 100'000);
            STCPManager::postPoll(fdm, socket);
        }
        return socket.state.load();
    }

    // A port that accepts connections, so a socket has something real to connect to.
    unique_ptr<STCPManager::Port> openTestPort(uint16_t& port)
    {
        port = BedrockTester::ports.getPort();
        return STCPManager::openPort("127.0.0.1:" + to_string(port));
    }

    void testRawIPNeverDefers()
    {
        // Nothing to look up, so even an ASYNC socket connects in the constructor. This is what
        // keeps the proxy socket, which is always given a literal address, on the old path.
        uint16_t port = 0;
        auto listener = openTestPort(port);

        STCPManager::Socket socket("127.0.0.1:" + to_string(port), false, STCPManager::Socket::ResolveMode::ASYNC);
        ASSERT_NOT_EQUAL(socket.state.load(), STCPManager::Socket::RESOLVING);
        ASSERT_TRUE(socket.s > 0);

        BedrockTester::ports.returnPort(port);
    }

    void testCachedHostNeverDefers()
    {
        // A host resolved recently answers from the cache, so the steady state doesn't touch the
        // resolver pool either.
        uint16_t port = 0;
        auto listener = openTestPort(port);
        const string host = "localhost:" + to_string(port);

        sockaddr_in addr;
        ASSERT_TRUE(SResolveHost(host, addr));

        STCPManager::Socket socket(host, false, STCPManager::Socket::ResolveMode::ASYNC);
        ASSERT_NOT_EQUAL(socket.state.load(), STCPManager::Socket::RESOLVING);
        ASSERT_TRUE(socket.s > 0);

        BedrockTester::ports.returnPort(port);
    }

    void testDeferredConnectCompletes()
    {
        // With an empty cache the lookup goes to the pool, and the connection is finished later by
        // postPoll rather than by the constructor.
        uint16_t port = 0;
        auto listener = openTestPort(port);
        const string host = "localhost:" + to_string(port);
        SClearResolveCache();

        STCPManager::Socket socket(host, false, STCPManager::Socket::ResolveMode::ASYNC);

        // Whether we caught it mid-lookup is a race we can't control, but either way there must be
        // no fd yet if it's still resolving.
        if (socket.state.load() == STCPManager::Socket::RESOLVING) {
            ASSERT_EQUAL(socket.s, -1);

            // Buffering while resolving is accepted and reports success, having sent nothing.
            ASSERT_TRUE(socket.send("hello"));
            ASSERT_FALSE(socket.sendBufferEmpty());
            ASSERT_TRUE(socket.recv());
        }

        ASSERT_EQUAL(pollUntilSettled(socket), STCPManager::Socket::CONNECTED);
        ASSERT_TRUE(socket.s > 0);

        BedrockTester::ports.returnPort(port);
    }

    void testFailedResolutionClosesSocket()
    {
        // A name that can't resolve has to surface as a dead socket rather than hanging forever,
        // because the constructor already returned and can't throw at this point.
        SClearResolveCache();
        STCPManager::Socket socket("nonexistent-probe-xyzzy.invalid:443", false, STCPManager::Socket::ResolveMode::ASYNC);

        ASSERT_EQUAL(pollUntilSettled(socket), STCPManager::Socket::CLOSED);
        ASSERT_TRUE(socket.connectFailure);
    }

    void testAbandonWhileResolving()
    {
        // Destroying a socket mid-lookup is the case that has to be safe: the worker is still
        // running and will write its result somewhere. Run enough of them to make a use-after-free
        // likely to be caught under ASAN.
        SClearResolveCache();
        for (int i = 0; i < 20; i++) {
            const string host = "abandoned-host-" + to_string(i) + ".invalid:443";
            STCPManager::Socket socket(host, false, STCPManager::Socket::ResolveMode::ASYNC);
        }
    }

    void testResolverOutlivesCaller()
    {
        // Same idea one layer down, without a socket in the picture: the Resolution has to stay
        // valid for the worker after the requester drops it.
        SClearResolveCache();
        for (int i = 0; i < 20; i++) {
            auto resolution = SResolver::getInstance().resolve("dropped-host-" + to_string(i) + ".invalid:443");
            ASSERT_TRUE(resolution->getFD() > 0);
        }

        // A resolution we do hold onto reports a result and wakes its pipe.
        auto resolution = SResolver::getInstance().resolve("localhost:443");
        const uint64_t giveUpAt = STimeNow() + 10'000'000;
        while (resolution->getState() == SResolver::Resolution::PENDING && STimeNow() < giveUpAt) {
            fd_map fdm;
            SFDset(fdm, resolution->getFD(), SREADEVTS);
            S_poll(fdm, 100'000);
        }
        ASSERT_EQUAL(resolution->getState(), SResolver::Resolution::RESOLVED);

        fd_map fdm;
        SFDset(fdm, resolution->getFD(), SREADEVTS);
        S_poll(fdm, 0);
        ASSERT_TRUE(SFDAnySet(fdm, resolution->getFD(), SREADEVTS));
        resolution->drain();
    }
} __AsyncResolve;
