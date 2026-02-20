#include <libstuff/SFluentdLogger.h>
#include <libstuff/SThread.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <syslog.h>
#include <unistd.h>

const char* SPriorityName(int priority)
{
    switch (priority) {
        case LOG_EMERG:   return "EMERG";

        case LOG_ALERT:   return "ALERT";

        case LOG_CRIT:    return "CRIT";

        case LOG_ERR:     return "ERROR";

        case LOG_WARNING: return "WARN";

        case LOG_NOTICE:  return "NOTICE";

        case LOG_INFO:    return "INFO";

        case LOG_DEBUG:   return "DEBUG";

        default:          return "UNKNOWN";
    }
}

SFluentdLogger::SFluentdLogger(const string& host, in_port_t port) : host(host), port(port), running(true),
    buffer(make_unique<SRingBuffer<FluentdLogRecord, SRINGBUFFER_DEFAULT_CAPACITY>>())
{
    auto [thread, future] = SThread(&SFluentdLogger::senderLoop, this);
    senderThread = move(thread);
}

SFluentdLogger::~SFluentdLogger()
{
    running.store(false);
    if (senderThread.joinable()) {
        senderThread.join();
    }
}

int SFluentdLogger::openSocket()
{
    int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd == -1) {
        return -1;
    }

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.data(), &addr.sin_addr);

    if (connect(fd, (struct sockaddr*) &addr, sizeof(addr)) == -1) {
        close(fd);
        return -1;
    }

    return fd;
}

bool SFluentdLogger::sendAll(int fd, const string& data)
{
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            return false;
        }
        sent += n;
    }
    return true;
}

void SFluentdLogger::senderLoop()
{
    int fd = -1;

    while (true) {
        auto entry = buffer->pop();

        if (!entry.has_value()) {
            if (!running.load()) {
                break;
            }
            this_thread::sleep_for(chrono::milliseconds(1));
            continue;
        }

        if (fd == -1) {
            fd = openSocket();
        }

        const auto& record = entry.value();
        if (!sendAll(fd, record.json)) {
            close(fd);
            fd = -1;
            syslog(record.priority, "%s", record.json.data());
        }
    }

    if (fd != -1) {
        close(fd);
    }
}

bool SFluentdLogger::log(int priority, string&& json)
{
    return buffer->push({priority, move(json)});
}
