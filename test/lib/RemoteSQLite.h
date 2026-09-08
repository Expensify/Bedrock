/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    RemoteSQLite.h
 * Path:    test/lib/RemoteSQLite.h
 * Pair:    RemoteSQLite.cpp
 *
 * INTENT
 *   A client-side SQLite subclass that forwards every read/write to a
 *   BedrockTester's server over the network as a `Query` command instead of
 *   touching a local DB file, so HC-Tree-mode tests (which forbid multiple
 *   local processes on the same DB) can still use SQLite-shaped call sites.
 *
 * OBJECTS
 *   RemoteSQLite - overrides SQLite's read/write/writeIdempotent family to serialize each call
 *                  into a `Query` command and send it via the owning BedrockTester.
 *   RemoteSQLite::_runRemoteQuery - private helper; builds and sends the Query command with
 *                  bound parameters as `sql-param-<name>` headers, shared by all the public overrides.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; it's test-only infrastructure paired with BedrockTester in test/lib.
 *
 * NAMING QUALITY
 *   Consistent with the SQLite base class's naming and repo convention (`_` prefix on the
 *   private member and helper).
 * ─────────────────────────────────────────────────────────────────────*/
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

private:
    mutable BedrockTester* _tester;

    // Build a Query command with named bound parameters serialized into `sql-param-<name>` headers and
    // send it via the tester's executeWaitMultipleData. This bypasses BedrockTester::readDB's
    // remoteMode/HCTree gate so RemoteSQLite always forwards to the server, which is the only behavior
    // that makes sense for a class named "Remote".
    bool _runRemoteQuery(const string& query, const map<string, Parameter>& params, SQResult& result) const;
};
