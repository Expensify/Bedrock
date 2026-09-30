#pragma once

#include <chrono>
#include <cstddef>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace tpunit {
using namespace std;

// All strings passed to a writer are valid for the duration of the callback. Callbacks
// may come from test worker threads, but the runner serializes their delivery.
class OutputWriter {
public:
    struct PlannedFixture
    {
        size_t id;
        string name;
        size_t testCount;
    };

    struct RunResult
    {
        int passes;
        int failures;
        const set<string>& failureNames;
        const vector<pair<chrono::milliseconds, string>>& testTimes;
    };

    virtual ~OutputWriter() = default;

    virtual void runStarted(const vector<PlannedFixture>& fixtures) = 0;
    virtual void fixtureStarted(size_t id, const string& fixture, bool singleThreaded) = 0;
    virtual void fixtureFinished(size_t id, const string& fixture, chrono::milliseconds duration) = 0;
    virtual void fixtureSetupFailed(size_t id, const string& fixture) = 0;
    virtual void fixtureTeardownFailed(size_t id, const string& fixture) = 0;
    virtual void testStarted(size_t id, const string& fixture, const string& test) = 0;
    virtual void testFinished(size_t id, const string& fixture, const string& test, bool passed,
                              chrono::milliseconds duration, const string& bufferedInfo) = 0;
    virtual void assertionFailed(const string& fixture, const string& test, int number,
                                 const string& file, int line, const string& bufferedInfo) = 0;
    virtual void exceptionCaught(const string& fixture, const string& test, int number,
                                 const string& method, const string& cause,
                                 const string& bufferedInfo) = 0;
    virtual void trace(const string& fixture, const string& test, int number,
                       const string& file, int line, const string& message,
                       const string& bufferedInfo) = 0;
    virtual void comparisonFailed(const string& fixture, const string& test,
                                  const string& lhs, const string& rhs, bool isEqual) = 0;
    virtual void diagnostic(const string& fixture, const string& test, const string& message) = 0;
    virtual void invalidPattern(const string& pattern) = 0;
    virtual void unnamedFixture() = 0;
    virtual void unmatchedPattern(const string& pattern) = 0;
    virtual void threadShutdown(int threadID) = 0;
    virtual void runFinished(const RunResult& result) = 0;
};
}
