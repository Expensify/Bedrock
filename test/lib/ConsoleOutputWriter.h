#pragma once

#include <test/lib/OutputWriter.h>
#include <ostream>

namespace tpunit {
class ConsoleOutputWriter : public OutputWriter {
public:
    explicit ConsoleOutputWriter(bool verbose = false);
    ConsoleOutputWriter(std::ostream& output, bool verbose = false);

    void runStarted(const std::vector<PlannedFixture>& fixtures) override;
    void fixtureStarted(size_t id, const std::string& fixture, bool singleThreaded) override;
    void fixtureFinished(size_t id, const std::string& fixture, std::chrono::milliseconds duration) override;
    void fixtureSetupFailed(size_t id, const std::string& fixture) override;
    void fixtureTeardownFailed(size_t id, const std::string& fixture) override;
    void testStarted(size_t id, const std::string& fixture, const std::string& test) override;
    void testFinished(size_t id, const std::string& fixture, const std::string& test, bool passed,
                      std::chrono::milliseconds duration, const std::string& bufferedInfo) override;
    void assertionFailed(const std::string& fixture, const std::string& test, int number,
                         const std::string& file, int line, const std::string& bufferedInfo) override;
    void exceptionCaught(const std::string& fixture, const std::string& test, int number,
                         const std::string& method, const std::string& cause,
                         const std::string& bufferedInfo) override;
    void trace(const std::string& fixture, const std::string& test, int number,
               const std::string& file, int line, const std::string& message,
               const std::string& bufferedInfo) override;
    void comparisonFailed(const std::string& fixture, const std::string& test,
                          const std::string& lhs, const std::string& rhs, bool isEqual) override;
    void diagnostic(const std::string& fixture, const std::string& test, const std::string& message) override;
    void invalidPattern(const std::string& pattern) override;
    void unnamedFixture() override;
    void unmatchedPattern(const std::string& pattern) override;
    void threadShutdown(int threadID) override;
    void runFinished(const RunResult& result) override;

private:
    void breakCheckLine();

    std::ostream& output;
    bool verbose;
    int shortOutputColumn = 0;
};
}
