/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    PortMap.h
 * Path:    test/lib/PortMap.h
 * Pair:    PortMap.cpp
 *
 * INTENT
 *   Hands out non-conflicting TCP ports (from a fixed range) to BedrockTester
 *   instances, so parallel tests each spinning up their own server don't
 *   collide, and reuses returned ports once they're confirmed free again.
 *
 * OBJECTS
 *   PortMap - allocates ports sequentially from [START_PORT, MAX_PORT], recycling returned ones,
 *             and checks a port is actually bindable before handing it out.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent with repo convention (`_` prefix on private members). Not `S`-prefixed despite
 *   being a small shared utility type, but it's test-only infrastructure rather than a libstuff
 *   type, so the repo's `S`-prefix convention for shared utility types doesn't obviously apply.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
#include <libstuff/libstuff.h>

// Track TCP ports to use with the tester.
class PortMap {
public:

    static const int64_t START_PORT = 10000;
    static const int64_t MAX_PORT = 20000;

    // Constructor/Destructor
    PortMap(uint16_t from = START_PORT);
    ~PortMap();

    // Get an unused port
    uint16_t getPort();

    // Free up previously used port to be used again
    void returnPort(uint16_t port);

    // Waits for a particular port to be free to bind to. This is useful when we've killed a server, because sometimes
    // it takes the OS a few seconds to make the port available again.
    int waitForPort(uint16_t port);

private:
    uint16_t _from;
    set<uint16_t> _returned;
    mutex _m;
};
