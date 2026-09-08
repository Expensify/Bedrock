/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    AutoScopeOnPrepare.h
 * Path:    libstuff/AutoScopeOnPrepare.h
 * Pair:    AutoScopeOnPrepare.cpp
 *
 * INTENT
 *   RAII guard that installs an on-prepare callback on a SQLite object for
 *   the lifetime of the guard, then removes it on scope exit, so callers
 *   don't have to remember to unset the handler on every return path.
 *
 * OBJECTS
 *   AutoScopeOnPrepare  - holds a reference to the SQLite db and the
 *                         handler function pointer; constructor installs
 *                         and enables the handler (if `enable`), destructor
 *                         clears it (if `enable`).
 *
 * OUT OF PLACE
 *   [CANDIDATE] This is a libstuff file whose sole purpose is scoping a
 *   sqlitecluster/SQLite feature (setOnPrepareHandler/enablePrepareNotifications)
 *   and it #includes sqlitecluster/SQLite.h directly. libstuff is meant to
 *   be generic, dependency-light utility code; this unit is specific to
 *   one SQLite feature and arguably belongs in sqlitecluster alongside
 *   SQLite itself.
 *
 * NAME/LOCATION FIT
 *   Name describes the contents well. Directory is questionable — see
 *   OUT OF PLACE.
 *
 * NAMING QUALITY
 *   The constructor parameter and member are both `_db`/`db`, and the
 *   handler signature repeats a parameter also named `_db` distinct from
 *   the member of the same name (shadowing, though harmless here since
 *   only the pointer's type/signature is used, not the parameter itself).
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
#include <sqlitecluster/SQLite.h>
using namespace std;

// RAII-style mechanism for automatically setting and unsetting an on prepare handler
class AutoScopeOnPrepare {
public:
    AutoScopeOnPrepare(bool enable, SQLite& db, void(*handler)(SQLite & _db, int64_t tableID));
    ~AutoScopeOnPrepare();

private:
    bool _enable;
    SQLite& _db;
    void (*_handler)(SQLite& _db, int64_t tableID);
};
