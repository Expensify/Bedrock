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
    void _deleteFinishedJob(SQLite& db, const string& safeJobID);
};
