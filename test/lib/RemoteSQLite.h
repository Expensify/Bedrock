#pragma once

#include <sqlitecluster/SQLite.h>

class BedrockTester;

// RemoteSQLite is a client-side Proxy implementation of the SQLite base class.
// Instead of performing operations on a local database file, this class serializes read and write operations into `Query` commands
// and forwards them to a remote server for execution. Consequently, the actual database logic is handled by the remote host.
// This is particularly useful for testing in environments where multiple processes cannot access the database simultaneously (HCTree).
//
// Bound-parameter caveat: the forwarding wire format is just `query` text — it does not carry the params map. Overrides that take
// a params map below ignore it (with a SWARN if non-empty). If a remote-mode test ever needs bound-param semantics, expand the
// params into SQL literals locally first (sqlite3_expanded_sql) before reaching SQLite::read/write.
class RemoteSQLite : public SQLite {
public:
    RemoteSQLite(BedrockTester* tester);
    RemoteSQLite(const RemoteSQLite& from);

    bool read(const string& query, SQResult& result, bool skipInfoWarn = false) const override;
    bool read(const string& query, const map<string, Parameter>& params, SQResult& result, bool skipInfoWarn = false) const override;
    string read(const string& query, const map<string, Parameter>& params) const override;
    int read(const string& query, const map<string, Parameter>& params, sqlite3_qrf_spec* spec) const override;

    bool writeIdempotent(const string& query) override;
    bool writeIdempotent(const string& query, SQResult& result) override;
    bool writeIdempotent(const string& query, const map<string, Parameter>& params) override;
    bool writeIdempotent(const string& query, const map<string, Parameter>& params, SQResult& result) override;

    bool write(const string& query, const map<string, Parameter>& params) override;
    bool write(const string& query, const map<string, Parameter>& params, SQResult& result) override;

private:
    mutable BedrockTester* _tester;
};
