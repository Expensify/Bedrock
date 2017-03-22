#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

// Declare the class we're going to implement below
class BedrockPlugin_Jobs : public BedrockPlugin {
  public:
    // Implement base class interface
    virtual string getName() { return "Jobs"; }
    virtual void upgradeDatabase(BedrockNode* node, SQLite& db);
    virtual bool peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command);
    virtual bool processCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command);
    virtual void test(BedrockTester* tester);

  private:
    // Helper functions
    string _constructNextRunDATETIME(const string& lastScheduled, const string& lastRun, const string& repeat);
    bool _validateRepeat(const string& repeat) { return !_constructNextRunDATETIME("", "", repeat).empty(); }
    bool _hasPendingChildJobs(SQLite& db, int64_t jobID);

    // Merge two JSON objects by adding or replacing pairs that exist in `fromJSON` to `intoJSON`. This will return a
    // string representing a JSON object that contains the union of the keys in intoJSON and fromJSON. The value for
    // each key will be the value of the corresponding key in `fromJSON`, if it was set, or the value of the
    // corresponding key in `intoJSON`, if that key was not set in `fromJSON`. Note that this is *not* a recursive
    // algorithm.
    string _mergeJSON_addOrReplace(const string& intoJSON, const string& fromJSON);
};
