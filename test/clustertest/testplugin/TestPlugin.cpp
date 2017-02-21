#include "TestPlugin.h"

extern "C" void BEDROCK_PLUGIN_REGISTER_TESTPLUGIN() {
    // Register the global instance
    new BedrockPlugin_TestPlugin();
}


bool TestHTTPSMananager::_onRecv(Transaction* transaction) {
    string methodLine = transaction->fullResponse.methodLine;
    transaction->response = 0;
    size_t offset = methodLine.find_first_of(' ', 0);
    offset = methodLine.find_first_not_of(' ', offset);
    if (offset != string::npos) {
        int status = SToInt(methodLine.substr(offset));
        if (status) {
            transaction->response = status;
        }
    }
    if (!transaction->response) {
        transaction->response = 400;
        SWARN("Failed to parse method line from request: " << methodLine);
    }

    return false;
}

void BedrockPlugin_TestPlugin::initialize(const SData& args) {
    httpsManagers.push_back(&httpsManager);
}

TestHTTPSMananager::~TestHTTPSMananager() {
}

TestHTTPSMananager::Transaction* TestHTTPSMananager::send(const string& url, const SData& request) {
    return _httpsSend(url, request);
}


bool BedrockPlugin_TestPlugin::peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command) {
    // This should never exist when calling peek.
    SASSERT(!command->httpsRequest);
    if (command->request.methodLine == "testcommand") {
        command->response.methodLine = "200 OK";
        command->response.content = "this is a test response";
        return true;
    } else if (command->request.methodLine == "sendrequest") {
        SData request("GET / HTTP/1.1");
        request["Host"] = "www.expensify.com";
        command->httpsRequest = httpsManager.send("https://www.expensify.com/", request);
    }

    return false;
}

bool BedrockPlugin_TestPlugin::processCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command) {
    if (command->httpsRequest) {
        // If we're calling `process` on a command with a https request, it had better be finished.
        SASSERT(command->httpsRequest->finished);
        command->response.methodLine = to_string(command->httpsRequest->response);
        command->response.content = command->httpsRequest->fullResponse.content;
    } else {
        // Some non-http command, I guess.
        command->response.methodLine = "200 OK";
    }

    return true;
}
