#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

class BedrockPlugin_Jobs : public BedrockPlugin {
  friend class BedrockJobsCommand;
  public:
    BedrockPlugin_Jobs(BedrockServer& s);
    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand);
    virtual const string& getName() const;
    virtual void upgradeDatabase(SQLite& db);

    // We were using MAX_SIZE_SMALL in GetJob to check the job name, but now GetJobs accepts more than one job name,
    // because of that, we need to increase the size of the param to be able to accept around 50 job names.
    static constexpr int64_t MAX_SIZE_NAME = 255 * 50;

    // Set of supported verbs for jobs with case-insensitive matching.
    static const set<string,STableComp>supportedRequestVerbs;

  private:
    static int64_t getNextID(SQLite& db);
    static const string name;
};

class BedrockJobsCommand : public BedrockCommand {
  public:
    BedrockJobsCommand(SQLiteCommand&& baseCommand, BedrockPlugin_Jobs* plugin);
    virtual bool peek(SQLite& db);
    virtual void process(SQLite& db);
    virtual void handleFailedReply();

  private:
    // Helper functions
    string _constructNextRunDATETIME(const string& lastScheduled, const string& lastRun, const string& repeat);
    bool _validateRepeat(const string& repeat) { return !_constructNextRunDATETIME("", "", repeat).empty(); }
    bool _hasPendingChildJobs(SQLite& db, int64_t jobID);
    void _validatePriority(const int64_t priority);
};
