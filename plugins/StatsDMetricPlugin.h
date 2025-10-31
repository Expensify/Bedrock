#pragma once
#include "libstuff/libstuff.h"
#include "BedrockMetricPlugin.h"

class StatsDMetricPlugin : public BedrockMetricPlugin {
  public:
    explicit StatsDMetricPlugin(const SData& args);
    ~StatsDMetricPlugin() override;

    const string& getName() const override;

  private:
    // Worker thread for draining and sending metrics.
    void _networkLoop();

    // Format one Metric into a StatsD line.
    string _formatLine(const Metric& m) const;

    // Join lines into datagrams, respecting max size, and send them.
    void _sendBatch(vector<string>& lines);

    // Parse and resolve the destination host:port.
    bool _resolveDestination();

    // Sanitization helpers.
    static string _sanitizeName(const string& name);
    static string _sanitizeTagKey(const string& key);
    static string _sanitizeTagValue(const string& value);

    // Destination and config
    string _destHostPort;              // from -statsdServer
    sockaddr_in _dest{};               // resolved destination
    bool _destResolved{false};

    size_t _maxDatagramBytes{1400};    // from -statsdMaxDatagramBytes (default 1400)
    size_t _maxBatch{512};             // per drain
    uint64_t _maxWaitMs{50};           // wait for batch accumulation

    thread _worker;
    string _name{"STATSD"};
};

// Dynamic loader entry point for .so usage.
extern "C" BedrockMetricPlugin* BEDROCK_METRIC_PLUGIN_REGISTER_STATSD(const SData& args);


