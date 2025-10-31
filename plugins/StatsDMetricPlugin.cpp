#include "plugins/StatsDMetricPlugin.h"
#include "libstuff/SFastBuffer.h"
#include "libstuff/SData.h"
#include <unistd.h>

#undef SLOGPREFIX
#define SLOGPREFIX "{" << getName() << "} "

const string StatsDMetricPlugin::name("StatsD");

extern "C" BedrockMetricPlugin* BEDROCK_METRIC_PLUGIN_REGISTER_STATSD(const SData& args)
{
    return new StatsDMetricPlugin(args);
}

StatsDMetricPlugin::StatsDMetricPlugin(const SData& args)
  : BedrockMetricPlugin(args)
{
    SDEBUG("Loaded StatsD metric plugin");
    _destHostPort = args["-statsdServer"];
    if (args.isSet("-statsdMaxDatagramBytes")) {
        _maxDatagramBytes = max<size_t>(1400, args.calcU64("-statsdMaxDatagramBytes"));
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
    return name;
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
    const string hostname = SGetHostName();
    const string name = _sanitizeName(hostname + "." + m.name);
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

    // Build datagrams up to _maxDatagramBytes and send each via a fresh socket
    SFastBuffer buffer;

    // Flushing is fire and forget, we don't check for a return from statsd
    auto flush = [&](SFastBuffer& payload){
        if (payload.empty()) {
            return;
        }
        int s = S_socket(_destHostPort, false, false, false);
        if (s < 0) {
            SWARN("Failed to create socket to " << _destHostPort << ", dropping batch");
            return;
        }
        bool result = S_sendconsume(s, payload);
        if (!result) {
            SWARN("Failed to send to " << _destHostPort << ", dropping batch");
            ::close(s);
            return;
        }
        ::close(s);
        return;
    };

    for (const string& line : lines) {
        const size_t needed = (buffer.empty() ? 0 : 1) + line.size();
        if (buffer.size() + needed > _maxDatagramBytes) {
            flush(buffer);
            buffer.clear();
        }

        if (!buffer.empty()) {
            buffer.append("\n", 1);
        }

        if (line.size() > _maxDatagramBytes) {
            // If a single line is larger than the datagram size, drop it.
            SWARN("Dropping line larger than datagram size: " << line.size());
            continue;
        }
        buffer += line;
    }
    flush(buffer);
    SDEBUG("Flushed " << lines.size() << " lines to " << _destHostPort);
}

void StatsDMetricPlugin::_networkLoop()
{
    while (!isStopping()) {
        SDEBUG("Waiting for batch");
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
