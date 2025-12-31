#pragma once
#include <cstdint>
#include <string>

class SQLite;

using namespace std;

class SQLiteUtils {
public:
    // Generates a random ID and checks the given tableName and column to ensure
    // uniqueness.
    static int64_t getRandomID(const SQLite& db, const string& tableName, const string& column);
};
