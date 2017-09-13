#include <libstuff/libstuff.h>
#include "BedrockPlugin.h"
#include "BedrockServer.h"

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
        STHROW("402 Missing " + name);
    }
    if (!request[name].empty() && request[name] != SToStr(SToInt64(request[name]))) {
        STHROW("402 Malformed " + name);
    }
}

void BedrockPlugin::verifyAttributeSize(const SData& request, const string& name, size_t minSize, size_t maxSize) {
    if (request[name].size() < minSize) {
        STHROW("402 Missing " + name);
    }
    if (request[name].size() > maxSize) {
        STHROW("402 Malformed " + name);
    }
}

void BedrockPlugin::verifyAttributeBool(const SData& request, const string& name, bool require)
{
    if (require && !request[name].size()) {
        STHROW("402 Missing " + name);
    }
    if (!request[name].empty() && !SIEquals(request[name], "true") && !SIEquals(request[name], "false")) {
        STHROW("402 Malformed " + name);
    }
}

STable BedrockPlugin::getInfo() {
    return STable();
}

string BedrockPlugin::getName() {
    SERROR("No name defined by this plugin, aborting.");
}

void BedrockPlugin::initialize(const SData& args, BedrockServer& server) {}

bool BedrockPlugin::peekCommand(SQLite& db, BedrockCommand& command) {
    return false;
}
bool BedrockPlugin::processCommand(SQLite& db, BedrockCommand& command) {
    return false;
}

void BedrockPlugin::timerFired(SStopwatch* timer) {}

void BedrockPlugin::upgradeDatabase(SQLite& db) {}

BedrockPlugin* BedrockPlugin::getPluginByName(const string& name) {
    // If our global list isn't set, there's no plugin to return.
    if (!g_registeredPluginList) {
        return nullptr;
    }

    // If we find our plugin in our list, we'll return it.
    for (auto& plugin : *g_registeredPluginList) {
        if (SIEquals(plugin->getName(), name)) {
            return plugin;
        }
    }

    // Didn't find it.
    return nullptr;
}
