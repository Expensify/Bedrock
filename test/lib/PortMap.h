// Track TCP ports to use with the tester.
class PortMap {
  public:

    static const int64_t START_PORT = 10000;
    static const int64_t MAX_PORT = 20000;

    // Get an unused port
    uint16_t getPort()
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

    // Free up previously used port to be used again
    void returnPort(uint16_t port)
    {
        lock_guard<mutex> lock(_m);
        _returned.insert(port);
    }

    // Waits for a particular port to be free to bind to. This is useful when we've killed a server, because sometimes
    // it takes the OS a few seconds to make the port available again.
    int waitForPort(int port) {
        int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        int i = 1;
        setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &i, sizeof(i));
        sockaddr_in addr = {0};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");

        int result = 0;
        int count = 0;
        uint64_t start = STimeNow();
        do {
            result = ::bind(sock, (sockaddr*)&addr, sizeof(addr));
            if (result) {
                count++;
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

    PortMap(uint16_t from = START_PORT) : _from(from)
    {}

  private:
    uint16_t _from;
    set<uint16_t> _returned;
    mutex _m;
};

