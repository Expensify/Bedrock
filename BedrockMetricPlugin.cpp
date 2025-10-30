#include "BedrockMetricPlugin.h"
#include "libstuff/libstuff.h"

std::map<std::string, std::function<BedrockMetricPlugin*(const SData& args)>> BedrockMetricPlugin::g_registeredMetricPluginList;

BedrockMetricPlugin::BedrockMetricPlugin(const SData& args, size_t maxQueueSize)
  : _args(args), _maxQueueSize(maxQueueSize)
{
}

BedrockMetricPlugin::~BedrockMetricPlugin()
{
    stop();
}

bool BedrockMetricPlugin::enqueue(Metric metric)
{
    if (_stopping.load()) {
        return false;
    }

    {
        std::unique_lock<std::mutex> lock(_mutex);
        if (_queue.size() >= _maxQueueSize) {
            _dropped.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        _queue.emplace_back(std::move(metric));
    }
    _cv.notify_one();
    return true;
}

size_t BedrockMetricPlugin::queueSize() const
{
    std::unique_lock<std::mutex> lock(_mutex);
    return _queue.size();
}

uint64_t BedrockMetricPlugin::droppedCount() const
{
    return _dropped.load(std::memory_order_relaxed);
}

void BedrockMetricPlugin::stop()
{
    bool expected = false;
    if (_stopping.compare_exchange_strong(expected, true)) {
        _cv.notify_all();
    }
}

bool BedrockMetricPlugin::isStopping() const
{
    return _stopping.load();
}

bool BedrockMetricPlugin::tryDequeue(Metric& out)
{
    std::unique_lock<std::mutex> lock(_mutex);
    if (_queue.empty()) {
        return false;
    }
    out = std::move(_queue.front());
    _queue.pop_front();
    return true;
}

bool BedrockMetricPlugin::waitDequeue(Metric& out, uint64_t timeoutMs)
{
    std::unique_lock<std::mutex> lock(_mutex);
    if (_queue.empty()) {
        if (_stopping.load()) {
            return false;
        }
        _cv.wait_for(lock, std::chrono::milliseconds(timeoutMs), [&]{ return _stopping.load() || !_queue.empty(); });
    }
    if (_queue.empty()) {
        return false;
    }
    out = std::move(_queue.front());
    _queue.pop_front();
    return true;
}

std::vector<Metric> BedrockMetricPlugin::drainUpTo(size_t maxItems)
{
    std::vector<Metric> batch;
    batch.reserve(maxItems);
    std::unique_lock<std::mutex> lock(_mutex);
    while (!_queue.empty() && batch.size() < maxItems) {
        batch.emplace_back(std::move(_queue.front()));
        _queue.pop_front();
    }
    return batch;
}

std::vector<Metric> BedrockMetricPlugin::waitAndDrain(size_t maxItems, uint64_t maxWaitMs)
{
    std::unique_lock<std::mutex> lock(_mutex);
    if (_queue.empty()) {
        if (_stopping.load()) {
            return {};
        }
        _cv.wait_for(lock, std::chrono::milliseconds(maxWaitMs), [&]{ return _stopping.load() || !_queue.empty(); });
    }
    std::vector<Metric> batch;
    batch.reserve(maxItems);
    while (!_queue.empty() && batch.size() < maxItems) {
        batch.emplace_back(std::move(_queue.front()));
        _queue.pop_front();
    }
    return batch;
}


