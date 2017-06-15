#include <libstuff/libstuff.h>
#include <bitset>

class BedrockConflictMetrics {
public:
    // Constructor takes the name of the command.
    BedrockConflictMetrics(const string& commandName);

    // Call to increment successes for this command.
    void success();

    // Call to increment conflicts for this command.
    void conflict();

    // Returns the count of recent successes for this command.
    int recentSuccessCount();

    // Returns the count of recent successes for this command.
    int recentConflictCount();

    // Record a conflict for the given command name.
    static void recordConflict(const string& commandName);

    // Record a successful commit for the given command name.
    static void recordSuccess(const string& commandName);

    // Returns whether or not commands with this name are currently able to be written in parallel.
    static bool multiWriteOK(const string& commandName);

    // Returns a comma-separated list of command names that are currently disabled due to conflicts.
    static string getMultiWriteDeniedCommands();

private:
    // The number of most recent commands to keep track of the results from.
    static constexpr int COMMAND_COUNT = 100;

    // The results of the most recent COMMAND_COUNT commands of this name.
    // A bit set to 0 is success. A bit set to 1 is conflict.
    bitset<COMMAND_COUNT> _results;

    // Pointer to the next spot in _results to update.
    int _resultsPtr = 0;

    // Total counts of this command's successes/conflicts since object creation.
    uint64_t _totalSuccessCount = 0;
    uint64_t _totalConflictCount = 0;

    // Name of this command.
    string _commandName;

    // This records what we returned for the last call to `multiWriteOK` for this command. This is for extra logging
    // when this value changes.
    bool _lastCheckOK = true;

    // Synchronization object.
    static recursive_mutex _mutex;

    // Map of commandNames to their BedrockConflictMetrics objects.
    static map<string, BedrockConflictMetrics> _conflictInfoMap;

    // The fraction of commands of a given name that are allowed to conflict before we decide the sync thread will
    // process them all.
    static constexpr double FRACTION = 0.10;

    // The count of conflicts of the last BedrockConflictMetrics::COMMAND_COUNT commands that we'll allow to have failed
    // before we decide that this command needs to be executed on the sync thread.
    static constexpr int THRESHOLD = FRACTION * COMMAND_COUNT;
};
