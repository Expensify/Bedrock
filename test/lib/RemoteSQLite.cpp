#include "RemoteSQLite.h"

#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

RemoteSQLite::RemoteSQLite(BedrockTester* tester) : SQLite(":memory:", 1000, 1000, -1, 0, false), _tester(tester)
{
};

RemoteSQLite::RemoteSQLite(const RemoteSQLite& from) : SQLite(from), _tester(from._tester)
{
}

bool RemoteSQLite::_runRemoteQuery(const string& query, const map<string, Parameter>& params, SQResult& result) const
{
    SData command("Query");
    command["Query"] = SEndsWith(query, ";") ? query : query + ";";
    command["Format"] = "json";
    command["ReadDBFlags"] = "-json";
    for (const auto& [name, value] : params) {
        command["sql-param-" + SQliteParameter::uriEncodeParamName(name)] = value.serialize();
    }
    auto responses = _tester->executeWaitMultipleData({command});
    if (responses[0].methodLine == "400 Unique Constraints Violation") {
        throw SQLite::constraint_error();
    }
    if (!SStartsWith(responses[0].methodLine, "200")) {
        return false;
    }
    if (!responses[0].content.empty()) {
        result.deserialize(responses[0].content);
    }
    return true;
}

bool RemoteSQLite::read(const string& query, SQResult& result, bool skipInfoWarn) const
{
    return _runRemoteQuery(query, {}, result);
}

bool RemoteSQLite::writeIdempotent(const string& query)
{
    SQResult ignore;
    return _runRemoteQuery(query, {}, ignore);
}

bool RemoteSQLite::writeIdempotent(const string& query, SQResult& result)
{
    return _runRemoteQuery(query, {}, result);
}

bool RemoteSQLite::read(const string& query, const map<string, Parameter>& params, SQResult& result, bool skipInfoWarn) const
{
    return _runRemoteQuery(query, params, result);
}

string RemoteSQLite::read(const string& query, const map<string, Parameter>& params) const
{
    SQResult result;
    if (!_runRemoteQuery(query, params, result)) {
        return "";
    }
    if (result.empty() || result[0].empty()) {
        return "";
    }
    return result[0][0];
}

int RemoteSQLite::read(const string& query, const map<string, Parameter>& params, sqlite3_qrf_spec* spec) const
{
    SQResult ignore;
    _runRemoteQuery(query, params, ignore);
    return SQLITE_OK;
}

bool RemoteSQLite::writeIdempotent(const string& query, const map<string, Parameter>& params)
{
    SQResult ignore;
    return _runRemoteQuery(query, params, ignore);
}

bool RemoteSQLite::writeIdempotent(const string& query, const map<string, Parameter>& params, SQResult& result)
{
    return _runRemoteQuery(query, params, result);
}

bool RemoteSQLite::write(const string& query, const map<string, Parameter>& params)
{
    SQResult ignore;
    return _runRemoteQuery(query, params, ignore);
}

bool RemoteSQLite::write(const string& query, const map<string, Parameter>& params, SQResult& result)
{
    return _runRemoteQuery(query, params, result);
}
