#pragma once

#include <sqlitecluster/SQLite.h>

class BedrockTester;

// RemoteSQLite is a client-side Proxy implementation of the SQLite base class.
// Instead of performing operations on a local database file, this class serializes read and write operations into `Query` commands
// and forwards them to a remote server for execution. Consequently, the actual database logic is handled by the remote host.
// This is particularly useful for testing in environments where multiple processes cannot access the database simultaneously (HCTree).
//
// Bound-parameter caveat: the forwarding wire format carries the params map as `sql-param-<name>` headers on the Query command,
// and the server-side DB plugin reassembles it before calling into SQLite. The leg that lacks schema-aware expansion (this side)
// never has to bind values itself — binding happens on the server where the real database lives.
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

    // Reads run on the remote server, so the base class counter never advances. Report the number of
    // read queries this proxy has forwarded instead, so tests can assert on read round-trips.
    int64_t getReadQueryCount() const override;

private:
    mutable BedrockTester* _tester;

    // Number of read queries forwarded to the server; each forwarded read is one round-trip.
    mutable int64_t _forwardedReadCount = 0;

    // Build a Query command with named bound parameters serialized into `sql-param-<name>` headers and
    // send it via the tester's executeWaitMultipleData. This bypasses BedrockTester::readDB's
    // remoteMode/HCTree gate so RemoteSQLite always forwards to the server, which is the only behavior
    // that makes sense for a class named "Remote".
    bool _runRemoteQuery(const string& query, const map<string, Parameter>& params, SQResult& result) const;
};
