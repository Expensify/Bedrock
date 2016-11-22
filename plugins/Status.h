/// /src/bedrock/plugins/Status.cpp
#include <libstuff/libstuff.h>
#include <libstuff/version.h>
#include "../BedrockPlugin.h"

// Declare the class we're going to implement below
class BedrockPlugin_Status : public BedrockPlugin {
  public:
    virtual string getName() { return "Status"; }
    virtual bool peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command);
};
