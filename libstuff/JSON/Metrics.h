/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    Metrics.h
 * Path:    libstuff/JSON/Metrics.h
 * Pair:    Metrics.cpp
 *
 * INTENT
 *   A narrow, process-wide hook so an embedding application can observe
 *   JSON parse/serialize timing and document size, without the JSON
 *   package depending on that application.
 *
 * OBJECTS
 *   MetricsOperation  - enum class; PARSE or SERIALIZE.
 *   MetricsObserver   - function-pointer type alias for the callback.
 *   setMetricsObserver  - free function; installs/replaces the callback
 *                         (nullptr disables reporting).
 *   reportMetrics       - free function; invoked by Parser/Writer after
 *                         each operation to notify the observer, if set.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Clear and consistent.
 * ─────────────────────────────────────────────────────────────────────*/

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
