#pragma once
#include <string>
#include <vector>
#include <utility>
#include <cstdint>

enum class MetricType {
    Counter,
    Gauge,
    Histogram,
    Timing,
    Set,
    Distribution
};

struct Metric {
    std::string name;
    MetricType type{MetricType::Counter};
    double value{0.0};
    std::vector<std::pair<std::string, std::string>> tags;
    uint64_t timestampUnixMs{0};
    double sampleRate{1.0};
};


