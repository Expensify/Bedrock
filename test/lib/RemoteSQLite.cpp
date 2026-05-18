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

bool RemoteSQLite::read(const string& query, const map<string, Parameter>& params, SQResult& result, bool skipInfoWarn) const
{
    return _tester->readDB(query, params, result, true);
}

string RemoteSQLite::read(const string& query, const map<string, Parameter>& params) const
{
    SQResult result;
    if (!_tester->readDB(query, params, result, true)) {
        return "";
    }
    if (result.empty() || result[0].empty()) {
        return "";
    }
    return result[0][0];
}

int RemoteSQLite::read(const string& query, const map<string, Parameter>& params, sqlite3_qrf_spec* spec) const
{
    SQResult result;
    _tester->readDB(query, params, result, true);
    return SQLITE_OK;
}

bool RemoteSQLite::writeIdempotent(const string& query, const map<string, Parameter>& params)
{
    SQResult result;
    _tester->readDB(query, params, result, true);
    return true;
}

bool RemoteSQLite::writeIdempotent(const string& query, const map<string, Parameter>& params, SQResult&)
{
    SQResult result;
    _tester->readDB(query, params, result, true);
    return true;
}

bool RemoteSQLite::write(const string& query, const map<string, Parameter>& params)
{
    SQResult result;
    _tester->readDB(query, params, result, true);
    return true;
}

bool RemoteSQLite::write(const string& query, const map<string, Parameter>& params, SQResult&)
{
    SQResult result;
    _tester->readDB(query, params, result, true);
    return true;
}
