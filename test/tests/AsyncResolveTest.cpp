#include <arpa/inet.h>
#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SResolver.h>
#include <libstuff/STCPManager.h>
#include <test/lib/BedrockTester.h>

struct AsyncResolve : tpunit::TestFixture
{
    AsyncResolve() : tpunit::TestFixture(true, "AsyncResolve",
                                         TEST(AsyncResolve::testRawIPNeverDefers),
                                         TEST(AsyncResolve::testGraceWindowConnectsInline),
                                         TEST(AsyncResolve::testDeferredConnectCompletes),
                                         TEST(AsyncResolve::testFailedResolutionClosesSocket),
                                         TEST(AsyncResolve::testAbandonWhileResolving),
                                         TEST(AsyncResolve::testResolverOutlivesCaller),
                                         TEST(AsyncResolve::testResolutionWakesPoll),
                                         TEST(AsyncResolve::testLiteralNeedsNoThread))
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

        listener.reset();
        BedrockTester::ports.returnPort(port);
    }

    void testGraceWindowConnectsInline()
    {
        // The grace wait is what replaced the in-process cache: a host the system resolver can
        // answer immediately comes back inside the window, so the socket connects in the
        // constructor and never reaches RESOLVING. `localhost` is served from /etc/hosts, which is
        // the fastest case there is.
        uint16_t port = 0;
        auto listener = openTestPort(port);

        // An explicitly generous grace period, because this is testing that the mechanism connects
        // inline when the answer arrives in time -- not whether the default 5ms happens to be
        // enough on a loaded test machine, which is a timing assertion and would flake.
        STCPManager::Socket socket("localhost:" + to_string(port), false, STCPManager::Socket::ResolveMode::ASYNC, 2000);
        ASSERT_NOT_EQUAL(socket.state.load(), STCPManager::Socket::RESOLVING);
        ASSERT_TRUE(socket.s > 0);

        listener.reset();
        BedrockTester::ports.returnPort(port);
    }

    void testDeferredConnectCompletes()
    {
        // A zero grace period forces the deferred path, so this doesn't depend on losing a race
        // with the resolver to be meaningful.
        uint16_t port = 0;
        auto listener = openTestPort(port);
        const string host = "localhost:" + to_string(port);

        STCPManager::Socket socket(host, false, STCPManager::Socket::ResolveMode::ASYNC, 0);
        ASSERT_EQUAL(socket.state.load(), STCPManager::Socket::RESOLVING);
        ASSERT_EQUAL(socket.s, -1);

        // Buffering while resolving is accepted and reports success, having sent nothing.
        ASSERT_TRUE(socket.send("hello"));
        ASSERT_FALSE(socket.sendBufferEmpty());
        ASSERT_TRUE(socket.recv());

        ASSERT_EQUAL(pollUntilSettled(socket), STCPManager::Socket::CONNECTED);
        ASSERT_TRUE(socket.s > 0);

        listener.reset();
        BedrockTester::ports.returnPort(port);
    }

    void testFailedResolutionClosesSocket()
    {
        // A name that can't resolve has to surface as a dead socket rather than hanging forever,
        // because the constructor already returned and can't throw at this point.
        STCPManager::Socket socket("nonexistent-probe-xyzzy.invalid:443", false, STCPManager::Socket::ResolveMode::ASYNC, 0);

        ASSERT_EQUAL(pollUntilSettled(socket), STCPManager::Socket::CLOSED);
        ASSERT_TRUE(socket.connectFailure);
    }

    void testResolutionWakesPoll()
    {
        // The point of registering the resolution's pipe is that poll() returns when the answer
        // lands rather than when it times out. A short poll timeout can't tell those apart, so this
        // uses a long one and measures how long we actually sat in it.
        //
        // Grace period 0 so the socket is guaranteed to defer, and an unresolvable host so the
        // answer takes a real round trip to arrive. A failed lookup writes the same pipe byte a
        // successful one does, which is the wake-up being tested.
        STCPManager::Socket socket("slow-to-fail-probe.invalid:443", false, STCPManager::Socket::ResolveMode::ASYNC, 0);
        ASSERT_EQUAL(socket.state.load(), STCPManager::Socket::RESOLVING);

        // prePoll has to contribute exactly one fd, and it can't be the socket's, because there
        // isn't one yet.
        fd_map fdm;
        STCPManager::prePoll(fdm, socket);
        ASSERT_EQUAL(socket.s, -1);
        ASSERT_EQUAL(fdm.size(), 1);
        ASSERT_FALSE(fdm.contains(socket.s));

        // Without the pipe registered this sits for the full five seconds.
        const uint64_t before = STimeNow();
        S_poll(fdm, 5'000'000);
        const uint64_t elapsedUS = STimeNow() - before;
        ASSERT_LESS_THAN(elapsedUS, 2'000'000);

        // And the byte we were woken by has to be the completed resolution.
        STCPManager::postPoll(fdm, socket);
        ASSERT_EQUAL(socket.state.load(), STCPManager::Socket::CLOSED);
        ASSERT_TRUE(socket.connectFailure);
    }

    void testAbandonWhileResolving()
    {
        // Destroying a socket mid-lookup is the case that has to be safe: the worker is still
        // running and will write its result somewhere. Run enough of them to make a use-after-free
        // likely to be caught under ASAN.
        for (int i = 0; i < 20; i++) {
            const string host = "abandoned-host-" + to_string(i) + ".invalid:443";
            STCPManager::Socket socket(host, false, STCPManager::Socket::ResolveMode::ASYNC, 0);
        }
    }

    void testResolverOutlivesCaller()
    {
        // Same idea one layer down, without a socket in the picture: the Resolution has to stay
        // valid for the worker after the requester drops it.
        for (int i = 0; i < 20; i++) {
            auto resolution = SResolve("dropped-host-" + to_string(i) + ".invalid:443");
            ASSERT_TRUE(resolution->getFD() > 0);
        }

        // A resolution we do hold onto reports a result and wakes its pipe.
        auto resolution = SResolve("localhost:443");
        const uint64_t giveUpAt = STimeNow() + 10'000'000;
        while (resolution->getState() == SResolution::PENDING && STimeNow() < giveUpAt) {
            fd_map fdm;
            SFDset(fdm, resolution->getFD(), SREADEVTS);
            S_poll(fdm, 100'000);
        }
        ASSERT_EQUAL(resolution->getState(), SResolution::RESOLVED);

        fd_map fdm;
        SFDset(fdm, resolution->getFD(), SREADEVTS);
        S_poll(fdm, 0);
        ASSERT_TRUE(SFDAnySet(fdm, resolution->getFD(), SREADEVTS));
        resolution->drain();
    }

    void testLiteralNeedsNoThread()
    {
        // A literal address is answered inline, so it never occupies a resolver thread and the
        // caller never has anything to wait for. This is what keeps sockets built from an IP --
        // the proxy socket, cluster peers -- on exactly the path they were on before.
        auto literal = SResolve("127.0.0.1:443");
        ASSERT_EQUAL(literal->getState(), SResolution::RESOLVED);
        ASSERT_EQUAL(literal->getAddr().sin_addr.s_addr, inet_addr("127.0.0.1"));
        ASSERT_EQUAL(ntohs(literal->getAddr().sin_port), 443);

        // A name can't be, so it starts out pending and finishes later.
        auto name = SResolve("needs-a-thread-probe.invalid:443");
        const uint64_t giveUpAt = STimeNow() + 30'000'000;
        while (name->getState() == SResolution::PENDING && STimeNow() < giveUpAt) {
            usleep(10'000);
        }
        ASSERT_EQUAL(name->getState(), SResolution::FAILED);
    }
} __AsyncResolve;
