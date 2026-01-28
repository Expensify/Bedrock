#pragma once

#include <sqlitecluster/SQLite.h>

class BedrockTester;

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
