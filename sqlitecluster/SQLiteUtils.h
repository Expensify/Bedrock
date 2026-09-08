/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SQLiteUtils.h
 * Path:    sqlitecluster/SQLiteUtils.h
 * Pair:    SQLiteUtils.cpp
 *
 * INTENT
 *   Generates a random, table-unique int64 ID for use as a primary key.
 *
 * OBJECTS
 *   SQLiteUtils  - static-only helper; getRandomID picks a random ID and retries until it's not already present in the given table/column
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   The generic name "Utils" for a single-function class is vague, but the
 *   function itself (SQLite-specific ID generation) belongs in sqlitecluster.
 *
 * NAMING QUALITY
 *   Fine as far as it goes; a one-function "Utils" class invites unrelated
 *   helpers to accrete here later.
 * ─────────────────────────────────────────────────────────────────────*/
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
