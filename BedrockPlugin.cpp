#include <libstuff/libstuff.h>
#include "BedrockPlugin.h"

// Global static values
list<BedrockPlugin*>* BedrockPlugin::g_registeredPluginList = 0;

BedrockPlugin::BedrockPlugin() : _enabled(false) {
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

BedrockPlugin* BedrockPlugin::getPlugin(const string& name) {
    list<BedrockPlugin*>::iterator plugin;
    plugin = find_if(g_registeredPluginList->begin(), g_registeredPluginList->end(),
                     [&](BedrockPlugin* plugin) { return SIEquals(plugin->getName(), name); });
    // Return what we found, or null if nothing.
    return (plugin == g_registeredPluginList->end()) ? 0 : *plugin;
}

// One-liner default implementations.
void BedrockPlugin::enable(bool enabled) { _enabled = enabled; }
bool BedrockPlugin::enabled() { return _enabled; }
string BedrockPlugin::getName() { SERROR("No name defined by this plugin, aborting."); }
const STable& BedrockPlugin::getInfo() { return pluginInfo; }
void BedrockPlugin::initialize(const SData& args) {}
bool BedrockPlugin::peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command) { return false; }
bool BedrockPlugin::processCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command) { return false; }
void BedrockPlugin::test(BedrockTester* tester) {}
void BedrockPlugin::timerFired(SStopwatch* timer) {}
void BedrockPlugin::upgradeDatabase(BedrockNode* node, SQLite& db) {}
