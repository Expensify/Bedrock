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
