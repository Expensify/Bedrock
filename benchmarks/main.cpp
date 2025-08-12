#include <iostream>
#include <map>
#include <set>
#include <cstdlib>
#include <iomanip>
#include <algorithm>
#include <test/lib/tpunit++.hpp>
#include "BenchmarkBase.h"

using namespace std;

// Define the global benchmark results storage
map<string, BenchmarkResult> g_benchmarkResults;

// ANSI color codes for terminal output
const string GREEN = "\033[32m";
const string RED = "\033[31m";
const string RESET = "\033[0m";
const string BOLD = "\033[1m";

const double SIGNIFICANT_CHANGE_THRESHOLD = 5.0;

bool runBenchmarks(const set<string>& include, const set<string>& exclude) {
    g_benchmarkResults.clear();
    int result = tpunit::Tests::run(include, exclude, {}, {}, 1);
    return result == 0;
}

bool executeCommand(const string& cmd, string* output = nullptr) {
    string fullCmd = cmd;
    if (output) {
        fullCmd += " 2>&1";  // Capture stderr too when output is requested
    }

    FILE* pipe = popen(fullCmd.c_str(), "r");
    if (!pipe) {
        return false;
    }

    char buffer[256];
    string result;
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        result += buffer;
        if (!output) {
            cout << buffer;  // Stream output in real-time if not capturing
        }
    }

    if (output) {
        *output = result;
    }

    int status = pclose(pipe);
    // pclose returns -1 on error, otherwise the exit status
    if (status == -1) {
        return false;
    }

    // On success, WEXITSTATUS should be 0
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

void printComparison(const map<string, BenchmarkResult>& baseline,
                     const map<string, BenchmarkResult>& current) {
    cout << "\n" << BOLD << "=== Benchmark Comparison Report ===" << RESET << "\n\n";
    
    // Find the longest benchmark name for formatting
    size_t maxNameLen = 0;
    for (const auto& [name, _] : current) {
        maxNameLen = max(maxNameLen, name.length());
    }
    maxNameLen = max(maxNameLen, size_t(20)); // Minimum width
    
    // Print header
    cout << left << setw(maxNameLen + 2) << "Benchmark"
         << right << setw(15) << "Baseline MB/s"
         << setw(15) << "Current MB/s"
         << setw(12) << "Change %"
         << setw(15) << "Status" << "\n";
    cout << string(maxNameLen + 57, '-') << "\n";
    
    // Compare results
    for (const auto& [name, currentResult] : current) {
        auto it = baseline.find(name);
        if (it == baseline.end()) {
            // New benchmark
            cout << left << setw(maxNameLen + 2) << name
                 << right << setw(15) << "N/A"
                 << setw(15) << fixed << setprecision(2) << currentResult.throughputMBps
                 << setw(12) << "NEW"
                 << setw(15) << "✨ NEW" << "\n";
        } else {
            const auto& baselineResult = it->second;
            double changePercent = ((currentResult.throughputMBps - baselineResult.throughputMBps) 
                                   / baselineResult.throughputMBps) * 100.0;
            
            // Use colors for significant changes (>5% difference)
            string color = "";
            string status = "";
            if (changePercent > SIGNIFICANT_CHANGE_THRESHOLD) {
                color = GREEN;
                status = "✅ IMPROVED";
            } else if (changePercent < (-1 * SIGNIFICANT_CHANGE_THRESHOLD)) {
                color = RED;
                status = "⚠️ REGRESSED";
            } else {
                status = "≈ UNCHANGED";
            }
            
            cout << left << setw(maxNameLen + 2) << name
                 << right << setw(15) << fixed << setprecision(2) << baselineResult.throughputMBps
                 << setw(15) << currentResult.throughputMBps
                 << color << setw(12) << showpos << setprecision(1) << changePercent << "%" << RESET
                 << noshowpos << setw(15) << status << "\n";
        }
    }
    
    // Check for removed benchmarks
    for (const auto& [name, baselineResult] : baseline) {
        if (current.find(name) == current.end()) {
            cout << left << setw(maxNameLen + 2) << name
                 << right << setw(15) << fixed << setprecision(2) << baselineResult.throughputMBps
                 << setw(15) << "N/A"
                 << setw(12) << "N/A"
                 << setw(15) << "❌ REMOVED" << "\n";
        }
    }
    
    cout << "\n";
}

int main(int argc, char* argv[]) {
    set<string> include;
    set<string> exclude;
    string baselineRef;
    
    // Parse arguments
    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        if (arg == "--baseline" && i + 1 < argc) {
            baselineRef = argv[++i];
        } else if (arg == "-only" && i + 1 < argc) {
            include.insert(argv[++i]);
        } else if (arg == "-except" && i + 1 < argc) {
            exclude.insert(argv[++i]);
        } else if (arg == "--help" || arg == "-h") {
            cout << "Usage: " << argv[0] << " [options]\n"
                 << "Options:\n"
                 << "  --baseline <ref>  Compare against a git ref (branch, tag, or commit)\n"
                 << "  -only <test>      Run only specified test\n"
                 << "  -except <test>    Run all tests except specified\n"
                 << "  --help            Show this help message\n";
            return 0;
        }
    }
    
    if (baselineRef.empty()) {
        // Normal benchmark run
        return runBenchmarks(include, exclude) ? 0 : 1;
    }
    
    // Baseline comparison mode
    cout << BOLD << "Running baseline comparison against: " << baselineRef << RESET << "\n\n";
    
    // Check for uncommitted changes
    string gitStatus;
    if (!executeCommand("git status --porcelain", &gitStatus)) {
        cerr << "Failed to check git status\n";
        return 1;
    }
    bool hasUncommitted = !gitStatus.empty();
    
    if (hasUncommitted) {
        cout << "Stashing uncommitted changes...\n";
        string stashOutput;
        if (!executeCommand("git stash", &stashOutput)) {
            cerr << "Failed to stash changes\n";
            cerr << "Output: " << stashOutput << "\n";
            return 1;
        }
    }
    
    // Get current branch/ref
    string currentRef;
    if (!executeCommand("git rev-parse --abbrev-ref HEAD", &currentRef)) {
        cerr << "Failed to get current ref\n";
        if (hasUncommitted) {
            executeCommand("git stash pop", nullptr);
        }
        return 1;
    }
    currentRef.erase(currentRef.find_last_not_of("\n\r") + 1); // trim newline
    
    // Checkout baseline and run benchmarks
    cout << "\n" << BOLD << "=== Running baseline benchmarks on " << baselineRef << " ===" << RESET << "\n\n";
    string checkoutOutput;
    if (!executeCommand("git checkout " + baselineRef, &checkoutOutput)) {
        cerr << "Failed to checkout baseline ref: " << baselineRef << "\n";
        cerr << "Output: " << checkoutOutput << "\n";
        if (hasUncommitted) {
            executeCommand("git stash pop", nullptr);
        }
        return 1;
    }
    
    // Rebuild with baseline code
    cout << "Building baseline...\n";
    if (!executeCommand("make bench -j32", nullptr)) {
        cerr << "Failed to build baseline\n";
        executeCommand("git checkout " + currentRef, nullptr);
        if (hasUncommitted) {
            executeCommand("git stash pop", nullptr);
        }
        return 1;
    }
    
    // Run baseline benchmarks
    cout << "\nRunning baseline benchmarks...\n";
    if (!runBenchmarks(include, exclude)) {
        cerr << "Baseline benchmarks failed\n";
        executeCommand("git checkout " + currentRef, nullptr);
        if (hasUncommitted) {
            executeCommand("git stash pop", nullptr);
        }
        return 1;
    }
    auto baselineResults = g_benchmarkResults;
    
    // Checkout current ref and run benchmarks
    cout << "\n" << BOLD << "=== Running current benchmarks on " << currentRef << " ===" << RESET << "\n\n";
    if (!executeCommand("git checkout " + currentRef, nullptr)) {
        cerr << "Failed to checkout current ref\n";
        if (hasUncommitted) {
            executeCommand("git stash pop", nullptr);
        }
        return 1;
    }
    
    // Restore stashed changes if any
    if (hasUncommitted) {
        cout << "Restoring stashed changes...\n";
        executeCommand("git stash pop", nullptr);
    }
    
    // Rebuild with current code
    cout << "Building current code...\n";
    if (!executeCommand("make bench -j32", nullptr)) {
        cerr << "Failed to build current code\n";
        return 1;
    }
    
    // Run current benchmarks
    cout << "\nRunning current benchmarks...\n";
    if (!runBenchmarks(include, exclude)) {
        cerr << "Current benchmarks failed\n";
        return 1;
    }
    auto currentResults = g_benchmarkResults;
    
    // Print comparison
    printComparison(baselineResults, currentResults);
    
    // Summary statistics
    int improved = 0, regressed = 0, unchanged = 0;
    for (const auto& [name, currentResult] : currentResults) {
        auto it = baselineResults.find(name);
        if (it != baselineResults.end()) {
            double changePercent = ((currentResult.throughputMBps - it->second.throughputMBps) 
                                   / it->second.throughputMBps) * 100.0;
            if (changePercent > SIGNIFICANT_CHANGE_THRESHOLD) {
                improved++;
            } else if (changePercent < (-1 * SIGNIFICANT_CHANGE_THRESHOLD)) {
                regressed++;
            } else {
                unchanged++;
            }
        }
    }
    
    cout << BOLD << "Summary: " << RESET
              << GREEN << improved << " improved" << RESET << ", "
              << RED << regressed << " regressed" << RESET << ", "
              << unchanged << " unchanged\n\n";
    
    return 0;
}
