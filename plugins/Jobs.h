#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

// Declare the class we're going to implement below
class BedrockPlugin_Jobs : public BedrockPlugin {

    class BedrockJobsCommand : public BedrockCommand {
      public:
        BedrockJobsCommand(BedrockPlugin_Jobs& _plugin, SData&& _request);
        virtual bool peek(SQLite& db);
        virtual void process(SQLite& db);
        virtual void handleFailedReply();
        virtual const string& getName() { return BedrockPlugin_Jobs::pluginName; }

      private:
        // Helper functions
        bool _isValidSQLiteDateModifier(const string& modifier);
        string _constructNextRunDATETIME(const string& lastScheduled, const string& lastRun, const string& repeat);
        bool _validateRepeat(const string& repeat) { return !_constructNextRunDATETIME("", "", repeat).empty(); }
        bool _hasPendingChildJobs(SQLite& db, int64_t jobID);
        BedrockPlugin_Jobs& plugin;
    };

  public:
    BedrockPlugin_Jobs(BedrockServer& s);

    // Return a new command.
    virtual unique_ptr<BedrockCommand> getCommand(SData&& request);

    // We were using MAX_SIZE_SMALL in GetJob to check the job name, but now GetJobs accepts more than one job name,
    // because of that, we need to increase the size of the param to be able to accept around 50 job names.
    static constexpr int64_t MAX_SIZE_NAME = 255 * 50;

    // Set of supported verbs for jobs with case-insensitive matching.
    static const set<string,STableComp>supportedRequestVerbs;

    // Implement base class interface
    virtual string getName() { return "Jobs"; }
    virtual void upgradeDatabase(SQLite& db);

  private:
    static int64_t getNextID(SQLite& db);
    static const string pluginName;
};
