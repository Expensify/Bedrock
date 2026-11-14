#pragma once

#include <cstddef>
#include <cstdint>

namespace JSON
{
enum class MetricsOperation : uint8_t
{
    PARSE,
    SERIALIZE,
};

/**
 * Optional callback for reporting JSON processing work to an embedding application.
 *
 * The callback receives the operation, elapsed time in microseconds, and the input
 * (parse) or output (serialize) size in bytes. Passing nullptr disables reporting.
 */
using MetricsObserver = void (*)(MetricsOperation operation, int64_t durationUS, size_t documentSize);

void setMetricsObserver(MetricsObserver observer);

// Used by the JSON implementation after a parse or serialization completes.
void reportMetrics(MetricsOperation operation, int64_t durationUS, size_t documentSize);
}
