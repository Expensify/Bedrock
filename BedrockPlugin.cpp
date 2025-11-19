#include <libstuff/libstuff.h>
#include "BedrockPlugin.h"
#include "BedrockServer.h"

map<string, function<BedrockPlugin* (BedrockServer&)>> BedrockPlugin::g_registeredPluginList;

BedrockPlugin::BedrockPlugin(BedrockServer& s) : server(s)
{
}

BedrockPlugin::~BedrockPlugin()
{
}

bool BedrockPlugin::isValidDate(const string& date)
{
    return SREMatch("^(19|2[0-9])\\d\\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])( \\d{2}:\\d{2}:\\d{2})?$", date);
}

void BedrockPlugin::verifyAttributeInt64(const SData& request, const string& name, size_t minSize)
{
    if (request[name].size() < minSize) {
        STHROW("402 Missing " + name);
    }
    if (!request[name].empty() && request[name] != SToStr(SToInt64(request[name]))) {
        STHROW("402 Malformed " + name);
    }
}

void BedrockPlugin::verifyAttributeSize(const SData& request, const string& name, size_t minSize, size_t maxSize)
{
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

void BedrockPlugin::verifyAttributeDate(const SData& request, const char* key, bool require)
{
    if (require && request[key].empty()) {
        STHROW("402 Missing " + string(key));
    }

    if (!request[key].empty() && !isValidDate(request[key])) {
        STHROW("402 Malformed " + string(key));
    }
}

STable BedrockPlugin::getInfo()
{
    return STable();
}

const string& BedrockPlugin::getName() const
{
    SERROR("No name defined by this plugin, aborting.");
}

bool BedrockPlugin::preventAttach()
{
    return false;
}

void BedrockPlugin::timerFired(SStopwatch* timer)
{
}

void BedrockPlugin::upgradeDatabase(SQLite& db)
{
}

bool BedrockPlugin::shouldLockCommitPageOnConflict(const string& conflictLocation) const
{
    return true;
}
