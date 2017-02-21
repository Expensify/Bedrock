#include "TestPlugin.h"

extern "C" void BEDROCK_PLUGIN_REGISTER_TESTPLUGIN() {
    // Register the global instance
    new BedrockPlugin_TestPlugin();
}

bool BedrockPlugin_TestPlugin::peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command) {
    if (command->request.methodLine == "testcommand") {
        command->response.methodLine = "200 OK";
        command->response.content = "this is a test response";
        return true;
    }

    return false;
}

bool BedrockPlugin_TestPlugin::processCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command) {
    return false;
}
