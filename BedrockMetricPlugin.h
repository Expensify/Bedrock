#pragma once
#include "libstuff/libstuff.h"
#include <deque>
#include <condition_variable>

enum class MetricType {
    Counter,
    Gauge,
    Histogram,
    Timing,
    Set,
    Distribution
};

struct Metric {
    string name;
    MetricType type{MetricType::Counter};
    uint64_t value{0};
    vector<pair<string, string>> tags;
    uint64_t timestampUnixMs{0};
    double sampleRate{1.0};
};

class BedrockMetricPlugin {
  public:
    static map<string, function<BedrockMetricPlugin*(const SData& args)>> g_registeredMetricPluginList;

    explicit BedrockMetricPlugin(const SData& args, size_t maxQueueSize = 100000);
    virtual ~BedrockMetricPlugin();

    virtual const string& getName() const = 0;

    // Non-blocking enqueue. Returns false if dropped due to full queue or stopping.
    bool enqueue(Metric metric);

    // Observability
    size_t queueSize() const;
    uint64_t droppedCount() const;

    // Lifecycle
    void stop();
    bool isStopping() const;

  protected:
    // For implementers: dequeue helpers to build network loop(s)
    bool tryDequeue(Metric& out);
    bool waitDequeue(Metric& out, uint64_t timeoutMs);
    vector<Metric> drainUpTo(size_t maxItems);
    vector<Metric> waitAndDrain(size_t maxItems, uint64_t maxWaitMs);

    const SData& _args;

  private:
    mutable mutex _mutex;
    condition_variable _cv;
    deque<Metric> _queue;
    atomic<bool> _stopping{false};
    const size_t _maxQueueSize;
    atomic<uint64_t> _dropped{0};
};



