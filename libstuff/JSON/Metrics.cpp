/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    Metrics.cpp
 * Path:    libstuff/JSON/Metrics.cpp
 * Pair:    Metrics.h
 *
 * INTENT
 *   Implements Metrics; see Metrics.h.
 *
 * OBJECTS
 *   (anonymous namespace) metricsObserver  - file-local atomic<MetricsObserver>;
 *   the actual process-wide storage backing setMetricsObserver()/
 *   reportMetrics(). Not declared in the header.
 *   setMetricsObserver / reportMetrics (impl)  - store/load metricsObserver
 *   with release/acquire ordering.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent with the header.
 * ─────────────────────────────────────────────────────────────────────*/

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
