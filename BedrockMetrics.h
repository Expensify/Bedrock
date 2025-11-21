// Minimal global metrics helper to allow emission from non-server code.
#pragma once
#include "BedrockMetricPlugin.h"
#include <functional>

// Set the global metric recorder callback.
void SetGlobalMetricRecorder(const function<void(const Metric&)>& recorder);

// Record a metric via the global recorder if configured.
void GlobalRecordMetric(const Metric& metric);
