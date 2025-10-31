#include "plugins/StatsDMetricPlugin.h"
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/socket.h>

StatsDMetricPlugin::StatsDMetricPlugin(const SData& args)
  : BedrockMetricPlugin(args)
{
    _destHostPort = _args["-statsdServer"];
    if (_args.isSet("-statsdMaxDatagramBytes")) {
        _maxDatagramBytes = max<size_t>(512, _args.calcU64("-statsdMaxDatagramBytes"));
    }
    // Start worker
    _worker = thread(&StatsDMetricPlugin::_networkLoop, this);
}

StatsDMetricPlugin::~StatsDMetricPlugin()
{
    stop();
    if (_worker.joinable()) {
        _worker.join();
    }
}

const string& StatsDMetricPlugin::getName() const
{
    return _name;
}

bool StatsDMetricPlugin::_resolveDestination()
{
    if (_destResolved) {
        return true;
    }
    if (_destHostPort.empty()) {
        SWARN("StatsD destination '-statsdServer' is not set; dropping metrics.");
        return false;
    }

    string host;
    string portStr;
    size_t colon = _destHostPort.rfind(':');
    if (colon == string::npos) {
        SWARN("StatsD destination missing port: '" << _destHostPort << "'");
        return false;
    }
    host = _destHostPort.substr(0, colon);
    portStr = _destHostPort.substr(colon + 1);

    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    addrinfo* result = nullptr;
    int rc = getaddrinfo(host.c_str(), portStr.c_str(), &hints, &result);
    if (rc != 0 || !result) {
        SWARN("getaddrinfo failed for StatsD server '" << _destHostPort << "': " << gai_strerror(rc));
        return false;
    }
    // Take first IPv4 result
    memcpy(&_dest, result->ai_addr, sizeof(sockaddr_in));
    freeaddrinfo(result);
    _destResolved = true;
    return true;
}

string StatsDMetricPlugin::_sanitizeName(const string& name)
{
    string out;
    out.reserve(name.size());
    for (char c : name) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.') {
            out.push_back(c);
        } else {
            out.push_back('_');
        }
    }
    return out;
}

string StatsDMetricPlugin::_sanitizeTagKey(const string& key)
{
    return _sanitizeName(key);
}

string StatsDMetricPlugin::_sanitizeTagValue(const string& value)
{
    string out = value;
    for (char& c : out) {
        if (c == '|') c = '_';
        else if (c == ' ') c = '_';
        else if (c == ',') c = '_';
        else if (c == '\n') c = '_';
        else if (c == ':') c = '_';
    }
    return out;
}

string StatsDMetricPlugin::_formatLine(const Metric& m) const
{
    const string name = _sanitizeName(m.name);
    string line = name + ":" + SToStr(m.value) + "|";
    switch (m.type) {
        case MetricType::Counter: line += "c"; break;
        case MetricType::Gauge: line += "g"; break;
        case MetricType::Timing: line += "ms"; break;
        case MetricType::Set: line += "s"; break;
        case MetricType::Histogram: line += "g"; break; // map to gauge
        case MetricType::Distribution: line += "g"; break; // map to gauge
    }
    if (m.sampleRate > 0.0 && m.sampleRate < 1.0) {
        line += "|@" + SToStr(m.sampleRate);
    }
    if (!m.tags.empty()) {
        line += "|#";
        bool first = true;
        for (const auto& kv : m.tags) {
            if (!first) {
                line += ",";
            }
            first = false;
            line += _sanitizeTagKey(kv.first);
            if (!kv.second.empty()) {
                line += ":" + _sanitizeTagValue(kv.second);
            }
        }
    }
    return line;
}

void StatsDMetricPlugin::_sendBatch(vector<string>& lines)
{
    if (lines.empty()) {
        return;
    }
    if (!_resolveDestination()) {
        return;
    }

    // Build datagrams up to _maxDatagramBytes and send each via a fresh socket
    string buffer;
    buffer.reserve(min<size_t>(_maxDatagramBytes, 4096));
    auto flush = [&](const string& payload){
        if (payload.empty()) return;
        int s = socket(AF_INET, SOCK_DGRAM, 0);
        if (s < 0) {
            SWARN("StatsD: failed to create UDP socket, dropping batch");
            return;
        }
        ssize_t sent = sendto(s, payload.data(), payload.size(), 0, (sockaddr*)&_dest, sizeof(_dest));
        if (sent < 0 || (size_t)sent != payload.size()) {
            SWARN("StatsD: sendto failed or partial, dropped: " << errno);
        }
        close(s);
    };

    for (const string& line : lines) {
        const size_t needed = (buffer.empty() ? 0 : 1) + line.size();
        if (buffer.size() + needed > _maxDatagramBytes) {
            flush(buffer);
            buffer.clear();
        }
        if (!buffer.empty()) buffer.push_back('\n');
        if (line.size() > _maxDatagramBytes) {
            // If a single line is larger than the datagram size, drop it.
            continue;
        }
        buffer += line;
    }
    flush(buffer);
}

void StatsDMetricPlugin::_networkLoop()
{
    while (!isStopping()) {
        vector<Metric> batch = waitAndDrain(_maxBatch, _maxWaitMs);
        if (batch.empty()) {
            continue;
        }
        vector<string> lines;
        lines.reserve(batch.size());
        for (const auto& m : batch) {
            lines.emplace_back(_formatLine(m));
        }
        _sendBatch(lines);
    }
}

extern "C" BedrockMetricPlugin* BEDROCK_METRIC_PLUGIN_REGISTER_STATSD(const SData& args)
{
    return new StatsDMetricPlugin(args);
}


