#include "PortMap.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include <iostream>

PortMap::PortMap(uint16_t from) : _from(from)
    {}

PortMap::~PortMap()
    {}

uint16_t PortMap::getPort()
{
    lock_guard<mutex> lock(_m);
    uint16_t port = 0;

    // Reuse the first available freed port.
    if (_returned.size()) {
        port = *_returned.begin();
        if (waitForPort(port) == 0) {
            _returned.erase(_returned.begin());
            return port;
        }
    }

    // Otherwise, grab from the next port in our range.
    do {
        port = _from;
        _from++;
        if (waitForPort(port) == 0) {
            return port;
        }
    } while (port<=MAX_PORT);

    // Should never reach this.
    cout << "Ran out of available ports after port " << port << endl;
    SASSERT(port<MAX_PORT);
    return 1;
}

void PortMap::returnPort(uint16_t port)
{
    lock_guard<mutex> lock(_m);
    _returned.insert(port);
}

int PortMap::waitForPort(uint16_t port) {
    int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    int i = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &i, sizeof(i));
    sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    int result = 0;
    uint64_t start = STimeNow();
    do {
        result = ::bind(sock, (sockaddr*)&addr, sizeof(addr));
        if (result) {
            usleep(100'000);
        } else {
            shutdown(sock, 2);
            close(sock);
            return 0;
        }
    // Wait up to 5 seconds.
    } while (result && STimeNow() < start + 5'000'000);

    // Ran out of time, return 1 ("unsuccessful")
    return 1;
}

