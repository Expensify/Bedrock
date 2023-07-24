#include "AutoScopeOnPrepare.h"

AutoScopeOnPrepare::AutoScopeOnPrepare(bool enable, SQLite& db, void (*handler)(SQLite& _db, int64_t tableID))
    : _enable(enable), _db(db), _handler(handler) {
    if (_enable) {
        _db.setOnPrepareHandler(_handler);
        _db.enablePrepareNotifications(true);
    }
}

AutoScopeOnPrepare ::~AutoScopeOnPrepare() {
    if (_enable) {
        _db.setOnPrepareHandler(nullptr);
        _db.enablePrepareNotifications(false);
    }
}
