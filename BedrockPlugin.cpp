#include <libstuff/libstuff.h>
#include "BedrockPlugin.h"
#include "BedrockServer.h"

// Global static values
list<BedrockPlugin*>* BedrockPlugin::g_registeredPluginList = nullptr;

BedrockPlugin::BedrockPlugin() {
    // Auto-register this instance into the global static list, initializing the list if that hasn't yet been done.
    // This just makes it available for enabling via the command line: by default all plugins start out disabled.
    //
    // NOTE: This code runs *before* main(). This means that libstuff hasn't yet been initialized, so there is no
    // logging.
    if (!g_registeredPluginList) {
        g_registeredPluginList = new list<BedrockPlugin*>;
    }
    g_registeredPluginList->push_back(this);
}

void BedrockPlugin::verifyAttributeInt64(const SData& request, const string& name, size_t minSize) {
    if (request[name].size() < minSize) {
        throw "402 Missing " + name;
    }
    if (!request[name].empty() && request[name] != SToStr(SToInt64(request[name]))) {
        throw "402 Malformed " + name;
    }
}

void BedrockPlugin::verifyAttributeSize(const SData& request, const string& name, size_t minSize, size_t maxSize) {
    if (request[name].size() < minSize) {
        throw "402 Missing " + name;
    }
    if (request[name].size() > maxSize) {
        throw "402 Malformed " + name;
    }
}

// One-liner default implementations.
STable BedrockPlugin::getInfo() { return STable(); }
string BedrockPlugin::getName() { SERROR("No name defined by this plugin, aborting."); }
void BedrockPlugin::initialize(const SData& args, BedrockServer& server) {}
bool BedrockPlugin::peekCommand(SQLite& db, BedrockCommand& command) { return false; }
bool BedrockPlugin::processCommand(SQLite& db, BedrockCommand& command) { return false; }
void BedrockPlugin::timerFired(SStopwatch* timer) {}
void BedrockPlugin::upgradeDatabase(SQLite& db) {}
