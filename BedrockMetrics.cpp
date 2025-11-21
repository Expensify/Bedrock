#include "BedrockMetrics.h"
#include <mutex>

namespace {
function<void(const Metric&)> g_recorder;
mutex g_mutex;
}

void SetGlobalMetricRecorder(const function<void(const Metric&)>& recorder)
{
    if (!recorder) {
        g_recorder = nullptr;
        return;
    }
    lock_guard<mutex> lock(g_mutex);
    g_recorder = recorder;
}

void GlobalRecordMetric(const Metric& metric)
{
    function<void(const Metric&)> local;
    {
        lock_guard<mutex> lock(g_mutex);
        local = g_recorder;
    }
    if (local) {
        local(metric);
    }
}
