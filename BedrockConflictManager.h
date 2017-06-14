#include <libstuff/libstuff.h>
#include <bitset>

class BedrockConflictInfo {
public:
    // The number of most recent commands to keep track of the results from.
    static constexpr int COMMAND_COUNT = 100;

    // Constructor takes the name of the command.
    BedrockConflictInfo(const string& commandName);

    // Call to increment successes.
    void success();

    // Call to increment conflicts.
    void conflict();

    // Returns the count of recent successes.
    int recentSuccessCount();

    // Returns the count of recent successes.
    int recentConflictCount();

    // Returns the count of all successful commits.
    uint64_t totalSuccessCount();

    // Returns the count of all conflicting commits.
    uint64_t totalConflictCount();

private:
    // The results of the most recent COMMAND_COUNT commands.
    bitset<COMMAND_COUNT> _results;

    // Pointer to the next spot in _results to update.
    int _resultsPtr = 0;

    // Total counts of commands since creation.
    uint64_t _totalSuccessCount;
    uint64_t _totalConflictCount;

    // Name of this command.
    string _commandName;
};

class BedrockConflictManager {
public:
    // Caller should call this when an attempt was made to commit that resulted in a conflict.
    static void commandConflicted(const string& commandName);

    // Caller should call this when an attempt was made to commit that succeeded.
    static void commandSucceeded(const string& commandName);

    // Returns whether or not commands with this name are currently able to be written in parallel.
    static bool multiWriteEnabled(const string& commandName);

private:
    // Synchronization object.
    static recursive_mutex _mutex;

    // Map of commandNames to their BedrockConflictInfo objects.
    static map<string, BedrockConflictInfo> _conflictInfoMap;

    // The fraction of commands of a given name that are allowed to conflict before we decide the sync thread will
    // process them all.
    static constexpr double _fraction = 0.10;

    // The number of conflicts of the last BedrockConflictInfo::COMMAND_COUNT commands that we'll allow to have failed
    // before we decide that this command needs to be executed on the sync thread.
    static int _threshold;
};
