/// /src/bedrock/plugins/Status.cpp
#include <libstuff/libstuff.h>
#include <libstuff/version.h>
#include "../BedrockPlugin.h"

class BedrockPlugin_Status : public BedrockPlugin {
  public:
    static const vector<string> statusCommandNames;
    virtual string getName() { return "Status"; }
    virtual bool peekCommand(SQLiteNode* node, SQLite& db, BedrockCommand* command);
    void initialize(const SData& args);

  private:
    const SData* _args = nullptr;
};
