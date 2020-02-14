#include <libstuff/libstuff.h>
#include "BedrockPlugin.h"
#include "BedrockServer.h"

map<string, function<BedrockPlugin*(BedrockServer&)>> BedrockPlugin::g_registeredPluginList;

BedrockPlugin::BedrockPlugin(BedrockServer& s) : server(s) {
}

BedrockPlugin::~BedrockPlugin() {
    for (auto httpsManager : httpsManagers) {
        delete httpsManager;
    }
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

const string& BedrockPlugin::getName() const {
    SERROR("No name defined by this plugin, aborting.");
}

bool BedrockPlugin::preventAttach() {
    return false;
}

void BedrockPlugin::timerFired(SStopwatch* timer) {}

void BedrockPlugin::upgradeDatabase(SQLite& db) {}
