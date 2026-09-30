#pragma once

#include <test/lib/OutputWriter.h>
#include <algorithm>
#include <iostream>
#include <ostream>
#include <sstream>

namespace tpunit {
class ConsoleOutputWriter : public OutputWriter {
public:
    explicit ConsoleOutputWriter(bool verbose = false);
    ConsoleOutputWriter(ostream& output, bool verbose = false);

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
    void breakCheckLine();

    ostream& output;
    bool verbose;
    int shortOutputColumn = 0;
};

inline ConsoleOutputWriter::ConsoleOutputWriter(bool verbose) : ConsoleOutputWriter(cout, verbose)
{
}

inline ConsoleOutputWriter::ConsoleOutputWriter(ostream& output, bool verbose)
    : output(output), verbose(verbose)
{
}

inline void ConsoleOutputWriter::breakCheckLine()
{
    if (!verbose && shortOutputColumn > 0) {
        output << '\n';
        shortOutputColumn = 0;
    }
}

inline void ConsoleOutputWriter::runStarted(const vector<PlannedFixture>&)
{
}

inline void ConsoleOutputWriter::fixtureStarted(size_t, const string&, bool singleThreaded)
{
    if (singleThreaded) {
        output << "--------------\n";
    }
}

inline void ConsoleOutputWriter::fixtureFinished(size_t, const string&, chrono::milliseconds)
{
}

inline void ConsoleOutputWriter::fixtureSetupFailed(size_t, const string& fixture)
{
    output << "\xE2\x9D\x8C !FAILED! \xE2\x9D\x8C initializing " << fixture << ". Skipping tests." << endl;
}

inline void ConsoleOutputWriter::fixtureTeardownFailed(size_t, const string& fixture)
{
    output << "\xE2\x9D\x8C !FAILED! \xE2\x9D\x8C cleaning up " << fixture << "." << endl;
}

inline void ConsoleOutputWriter::testStarted(size_t, const string&, const string&)
{
}

inline void ConsoleOutputWriter::testFinished(size_t, const string&, const string& test, bool passed,
                                              chrono::milliseconds duration, const string& bufferedInfo)
{
    ostringstream time;
    time << '(' << duration;
    if (duration > chrono::milliseconds(5000)) {
        time << " \xF0\x9F\x90\x8C";
    }
    time << ')';
    if (passed) {
        if (verbose) {
            output << "\xE2\x9C\x85 " << test << ' ' << time.str() << '\n';
        } else {
            if (shortOutputColumn >= 80) {
                breakCheckLine();
            }
            output << "\033[32m\xE2\x9C\x93\033[0m" << flush;
            ++shortOutputColumn;
        }
    } else {
        if (!verbose) {
            breakCheckLine();
        }
        output << bufferedInfo << "\xE2\x9D\x8C !FAILED! \xE2\x9D\x8C " << test << ' ' << time.str()
        << (verbose ? "\n" : "\n\n");
    }
}

inline void ConsoleOutputWriter::assertionFailed(const string&, const string&, int number,
                                                 const string& file, int line, const string& bufferedInfo)
{
    breakCheckLine();
    output << "   assertion #" << number << " at " << file << ':' << line << '\n' << bufferedInfo;
}

inline void ConsoleOutputWriter::exceptionCaught(const string&, const string&, int number,
                                                 const string& method, const string& cause,
                                                 const string& bufferedInfo)
{
    breakCheckLine();
    output << "   exception #" << number << " from " << method << " with cause: " << cause << '\n' << bufferedInfo;
}

inline void ConsoleOutputWriter::trace(const string&, const string&, int number,
                                       const string& file, int line, const string& message,
                                       const string& bufferedInfo)
{
    breakCheckLine();
    output << "   trace #" << number << " at " << file << ':' << line << ": " << message << '\n' << bufferedInfo;
}

inline void ConsoleOutputWriter::comparisonFailed(const string&, const string&,
                                                  const string& lhs, const string& rhs, bool isEqual)
{
    breakCheckLine();
    output << lhs << (isEqual ? " == " : " != ") << rhs << '\n';
}

inline void ConsoleOutputWriter::diagnostic(const string&, const string&, const string& message)
{
    output << message << endl;
}

inline void ConsoleOutputWriter::invalidPattern(const string& pattern)
{
    output << "Invalid pattern: " << pattern << ", skipping." << endl;
}

inline void ConsoleOutputWriter::unnamedFixture()
{
    output << "test has no name???" << endl;
}

inline void ConsoleOutputWriter::unmatchedPattern(const string& pattern)
{
    output << "\xE2\x9D\x8C Could not find any test matching, make sure the test name is right: " << pattern << '\n';
}

inline void ConsoleOutputWriter::threadShutdown(int threadID)
{
    output << "Thread " << threadID << " caught shutdown exception, exiting.\n";
}

inline void ConsoleOutputWriter::runFinished(const RunResult& result)
{
    output << "\n[ TEST RESULTS ] Passed: " << result.passes << ", Failed: " << result.failures << '\n';
    if (!result.failureNames.empty()) {
        output << "\nFailures:\n";
        for (const auto& failure : result.failureNames) {
            output << failure << '\n';
        }
    }
    output << "\nSlowest Test Classes: " << endl;
    long long totalTestTime = 0;
    for (const auto& testTime : result.testTimes) {
        totalTestTime += testTime.first.count();
    }
    auto testTimes = result.testTimes;
    stable_sort(testTimes.begin(), testTimes.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });
    for (size_t i = 0; i < min(size_t(10), testTimes.size()); ++i) {
        const auto& testTime = testTimes[testTimes.size() - i - 1];
        output << testTime.first << ": " << testTime.second << " : "
        << (static_cast<double>(testTime.first.count()) / totalTestTime) * 100.0
        << "% of total test time" << endl;
    }
    output << "Total test time across threads: " << totalTestTime << "ms" << endl;
}
}
