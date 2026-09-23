#pragma once

#include <libstuff/sqlite3.h>

// The bundled HC-Tree amalgamation exposes these APIs, but its standalone sqlite3.h
// does not contain their declarations. Values must match libstuff/sqlite3.c.
#define SQLITE_HCT_NORMAL   0
#define SQLITE_HCT_FOLLOWER 1

extern "C" {
int sqlite3_hct_journal_init(sqlite3* db);
int sqlite3_hct_journal_setmode(sqlite3* db, int mode);
int sqlite3_hct_journal_follower_commit(sqlite3* db, const unsigned char* data, int size,
                                        sqlite3_int64 cid, sqlite3_int64 snapshot);
int sqlite3_hct_journal_local_commit(sqlite3* db);
}
