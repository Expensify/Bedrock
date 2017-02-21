#include <libstuff/libstuff.h>
#include <BedrockPlugin.h>

class BedrockPlugin_TestPlugin : public BedrockPlugin
{
    virtual string getName() { return "TestPlugin"; }

    bool peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command);
    bool processCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command);
};
