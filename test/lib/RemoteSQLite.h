#pragma once

#include <sqlitecluster/SQLite.h>

class BedrockTester;

// RemoteSQLite is a client-side Proxy implementation of the SQLite base class.
// Instead of performing operations on a local database file, this class serializes read and write operations into `Query` commands
// and forwards them to a remote server for execution. Consequently, the actual database logic is handled by the remote host.
// This is particularly useful for testing in environments where multiple processes cannot access the database simultaneously (HCTree).
class RemoteSQLite : public SQLite {
public:
    RemoteSQLite(BedrockTester* tester);
    RemoteSQLite(const RemoteSQLite& from);

    bool read(const string& query, SQResult& result, bool skipInfoWarn = false) const override;
    bool writeIdempotent(const string& query) override;
    bool writeIdempotent(const string& query, SQResult& result) override;

private:
    mutable BedrockTester* _tester;
};
