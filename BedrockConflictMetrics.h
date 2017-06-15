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

    // Returns the count of all successful commits for this command.
    uint64_t totalSuccessCount();

    // Returns the count of all conflicting commits for this command.
    uint64_t totalConflictCount();

    // The number of most recent commands to keep track of the results from.
    static constexpr int COMMAND_COUNT = 100;

    // Record a conflict for the given command name.
    static void recordConflict(const string& commandName);

    // Record a successful commit for the given command name.
    static void recordSuccess(const string& commandName);

    // Returns whether or not commands with this name are currently able to be written in parallel.
    static bool multiWriteOK(const string& commandName);

private:
    // The results of the most recent COMMAND_COUNT commands of this name.
    bitset<COMMAND_COUNT> _results;

    // Pointer to the next spot in _results to update.
    int _resultsPtr = 0;

    // Total counts of this command's successes/conflicts since object creation.
    uint64_t _totalSuccessCount;
    uint64_t _totalConflictCount;

    // Name of this command.
    string _commandName;

    // Synchronization object.
    static recursive_mutex _mutex;

    // Map of commandNames to their BedrockConflictMetrics objects.
    static map<string, BedrockConflictMetrics> _conflictInfoMap;

    // The fraction of commands of a given name that are allowed to conflict before we decide the sync thread will
    // process them all.
    static constexpr double _fraction = 0.10;

    // The number of conflicts of the last BedrockConflictMetrics::COMMAND_COUNT commands that we'll allow to have failed
    // before we decide that this command needs to be executed on the sync thread.
    static int _threshold;
};
