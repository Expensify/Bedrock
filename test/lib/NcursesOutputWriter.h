#pragma once

#include <test/lib/OutputWriter.h>
#include <memory>

namespace tpunit {
// Owns the terminal until finish() restores stdout/stderr and prints the report.
class NcursesOutputWriter : public OutputWriter {
public:
    static bool available();

    NcursesOutputWriter();
    ~NcursesOutputWriter() override;
    void finish();

    void runStarted(const vector<PlannedFixture>& fixtures) override;
    void fixtureStarted(size_t id, const string& fixture, bool singleThreaded) override;
    void fixtureFinished(size_t id, const string& fixture, chrono::milliseconds duration) override;
    void fixtureSetupFailed(size_t id, const string& fixture) override;
    void fixtureTeardownFailed(size_t id, const string& fixture) override;
    void testStarted(size_t id, const string& fixture, const string& test) override;
    void testFinished(size_t id, const string& fixture, const string& test, bool passed,
                      chrono::milliseconds duration, const string& bufferedInfo) override;
    void assertionFailed(const string& fixture, const string& test, int number,
                         const string& file, int line, const string& bufferedInfo) override;
    void exceptionCaught(const string& fixture, const string& test, int number,
                         const string& method, const string& cause,
                         const string& bufferedInfo) override;
    void trace(const string& fixture, const string& test, int number,
               const string& file, int line, const string& message,
               const string& bufferedInfo) override;
    void comparisonFailed(const string& fixture, const string& test,
                          const string& lhs, const string& rhs, bool isEqual) override;
    void diagnostic(const string& fixture, const string& test, const string& message) override;
    void invalidPattern(const string& pattern) override;
    void unnamedFixture() override;
    void unmatchedPattern(const string& pattern) override;
    void threadShutdown(int threadID) override;
    void runFinished(const RunResult& result) override;

private:
    struct Impl;
    unique_ptr<Impl> impl;
};
}
