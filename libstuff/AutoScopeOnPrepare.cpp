/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    AutoScopeOnPrepare.cpp
 * Path:    libstuff/AutoScopeOnPrepare.cpp
 * Pair:    AutoScopeOnPrepare.h
 *
 * INTENT
 *   Implements AutoScopeOnPrepare; see the header for the public contract.
 *
 * OBJECTS
 *   AutoScopeOnPrepare::AutoScopeOnPrepare  - if enabled, calls
 *                            SQLite::setOnPrepareHandler and
 *                            SQLite::enablePrepareNotifications(true).
 *   AutoScopeOnPrepare::~AutoScopeOnPrepare - if enabled, clears the
 *                            handler and disables notifications.
 *
 * OUT OF PLACE
 *   Nothing beyond what's already noted in the header's OUT OF PLACE.
 *
 * NAME/LOCATION FIT
 *   Fits, see header.
 *
 * NAMING QUALITY
 *   Fits repo convention.
 * ─────────────────────────────────────────────────────────────────────*/
#include "AutoScopeOnPrepare.h"

AutoScopeOnPrepare::AutoScopeOnPrepare(bool enable, SQLite& db, void(*handler)(SQLite & _db, int64_t tableID))
    : _enable(enable), _db(db), _handler(handler)
{
    if (_enable) {
        _db.setOnPrepareHandler(_handler);
        _db.enablePrepareNotifications(true);
    }
}

AutoScopeOnPrepare ::~AutoScopeOnPrepare()
{
    if (_enable) {
        _db.setOnPrepareHandler(nullptr);
        _db.enablePrepareNotifications(false);
    }
}
