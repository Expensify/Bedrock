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
