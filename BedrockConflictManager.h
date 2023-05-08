#pragma once
#include <map>
#include <mutex>
#include <set>
#include <string>

using namespace std;

class BedrockConflictManagerCommandInfo {
  public:
    size_t count = 0;
    map<string, size_t> tableUseCounts;
};

class BedrockConflictManager {
  public:
    BedrockConflictManager();
    void recordTables(const string& commandName, const set<string>& tables);
    string generateReport();

  private:
    mutex m;
    map<string, BedrockConflictManagerCommandInfo> _commandInfo;
};
