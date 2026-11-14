#include "Metrics.h"

#include <atomic>

using namespace std;

namespace
{
atomic<JSON::MetricsObserver> metricsObserver{nullptr};
}

void JSON::setMetricsObserver(MetricsObserver observer)
{
    metricsObserver.store(observer, memory_order_release);
}

void JSON::reportMetrics(MetricsOperation operation, int64_t durationUS, size_t documentSize)
{
    if (const MetricsObserver observer = metricsObserver.load(memory_order_acquire)) {
        observer(operation, durationUS, documentSize);
    }
}
