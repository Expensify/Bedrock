#pragma once

#include <chrono>
#include <cstddef>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace tpunit {
// All strings passed to a writer are valid for the duration of the callback. Callbacks
// may come from test worker threads, but the runner serializes their delivery.
class OutputWriter {
public:
    struct PlannedFixture
    {
        size_t id;
        std::string name;
        size_t testCount;
    };

    struct RunResult
    {
        int passes;
        int failures;
        const std::set<std::string>& failureNames;
        const std::vector<std::pair<std::chrono::milliseconds, std::string>>& testTimes;
    };

    virtual ~OutputWriter() = default;

    virtual void runStarted(const std::vector<PlannedFixture>& fixtures) = 0;
    virtual void fixtureStarted(size_t id, const std::string& fixture, bool singleThreaded) = 0;
    virtual void fixtureFinished(size_t id, const std::string& fixture, std::chrono::milliseconds duration) = 0;
    virtual void fixtureSetupFailed(size_t id, const std::string& fixture) = 0;
    virtual void fixtureTeardownFailed(size_t id, const std::string& fixture) = 0;
    virtual void testStarted(size_t id, const std::string& fixture, const std::string& test) = 0;
    virtual void testFinished(size_t id, const std::string& fixture, const std::string& test, bool passed,
                              std::chrono::milliseconds duration, const std::string& bufferedInfo) = 0;
    virtual void assertionFailed(const std::string& fixture, const std::string& test, int number,
                                 const std::string& file, int line, const std::string& bufferedInfo) = 0;
    virtual void exceptionCaught(const std::string& fixture, const std::string& test, int number,
                                 const std::string& method, const std::string& cause,
                                 const std::string& bufferedInfo) = 0;
    virtual void trace(const std::string& fixture, const std::string& test, int number,
                       const std::string& file, int line, const std::string& message,
                       const std::string& bufferedInfo) = 0;
    virtual void comparisonFailed(const std::string& fixture, const std::string& test,
                                  const std::string& lhs, const std::string& rhs, bool isEqual) = 0;
    virtual void diagnostic(const std::string& fixture, const std::string& test, const std::string& message) = 0;
    virtual void invalidPattern(const std::string& pattern) = 0;
    virtual void unnamedFixture() = 0;
    virtual void unmatchedPattern(const std::string& pattern) = 0;
    virtual void threadShutdown(int threadID) = 0;
    virtual void runFinished(const RunResult& result) = 0;
};
}
