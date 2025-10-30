#pragma once
#include <deque>
#include <map>
#include <string>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <vector>
#include "libstuff/SData.h"
#include "BedrockMetric.h"

class BedrockMetricPlugin {
  public:
    static std::map<std::string, std::function<BedrockMetricPlugin*(const SData& args)>> g_registeredMetricPluginList;

    explicit BedrockMetricPlugin(const SData& args, size_t maxQueueSize = 100000);
    virtual ~BedrockMetricPlugin();

    virtual const std::string& getName() const = 0;

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
    std::vector<Metric> drainUpTo(size_t maxItems);
    std::vector<Metric> waitAndDrain(size_t maxItems, uint64_t maxWaitMs);

    const SData& _args;

  private:
    mutable std::mutex _mutex;
    std::condition_variable _cv;
    std::deque<Metric> _queue;
    std::atomic<bool> _stopping{false};
    const size_t _maxQueueSize;
    std::atomic<uint64_t> _dropped{0};
};


