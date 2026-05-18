#include "RemoteSQLite.h"

#include <test/lib/BedrockTester.h>

RemoteSQLite::RemoteSQLite(BedrockTester* tester) : SQLite(":memory:", 1000, 1000, -1, 0, false), _tester(tester)
{
};

RemoteSQLite::RemoteSQLite(const RemoteSQLite& from) : SQLite(from), _tester(from._tester)
{
}

bool RemoteSQLite::read(const string& query, SQResult& result, bool skipInfoWarn) const
{
    return _tester->readDB(query, result, true);
}

bool RemoteSQLite::writeIdempotent(const string& query)
{
    _tester->readDB(query);
    return true;
}

bool RemoteSQLite::writeIdempotent(const string& query, SQResult&)
{
    _tester->readDB(query);
    return true;
}

// Params-aware overrides. The forwarding protocol can't carry bound parameters, so any non-empty params map
// here means the test is asking for behavior we can't honor — the server would execute the SQL with the
// placeholders left unbound (NULL). Warn the test author rather than silently NULLing values.
static void warnIfNonEmptyParams(size_t size)
{
    if (size != 0) {
        SWARN("RemoteSQLite cannot forward bound parameters; placeholders will be NULL on the server. "
              "Expand params into SQL literals before calling.");
    }
}

bool RemoteSQLite::read(const string& query, const map<string, Parameter>& params, SQResult& result, bool skipInfoWarn) const
{
    warnIfNonEmptyParams(params.size());
    return _tester->readDB(query, result, true);
}

string RemoteSQLite::read(const string& query, const map<string, Parameter>& params) const
{
    warnIfNonEmptyParams(params.size());
    SQResult result;
    if (!_tester->readDB(query, result, true)) {
        return "";
    }
    if (result.empty() || result[0].empty()) {
        return "";
    }
    return result[0][0];
}

int RemoteSQLite::read(const string& query, const map<string, Parameter>& params, sqlite3_qrf_spec* spec) const
{
    warnIfNonEmptyParams(params.size());
    _tester->readDB(query);
    return SQLITE_OK;
}

bool RemoteSQLite::writeIdempotent(const string& query, const map<string, Parameter>& params)
{
    warnIfNonEmptyParams(params.size());
    _tester->readDB(query);
    return true;
}

bool RemoteSQLite::writeIdempotent(const string& query, const map<string, Parameter>& params, SQResult&)
{
    warnIfNonEmptyParams(params.size());
    _tester->readDB(query);
    return true;
}

bool RemoteSQLite::write(const string& query, const map<string, Parameter>& params)
{
    warnIfNonEmptyParams(params.size());
    _tester->readDB(query);
    return true;
}

bool RemoteSQLite::write(const string& query, const map<string, Parameter>& params, SQResult&)
{
    warnIfNonEmptyParams(params.size());
    _tester->readDB(query);
    return true;
}
