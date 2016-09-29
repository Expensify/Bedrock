// BedrockPlugin.h
#ifndef _BEDROCKPLUGIN_H
#define _BEDROCKPLUGIN_H
#include "BedrockNode.h"

// Define some helpful constants and macros
#define BVERIFY_ATTRIBUTE_INT(_ATTR_, _MIN_SIZE_)                                                                      \
    do {                                                                                                               \
        if (request[_ATTR_].size() < _MIN_SIZE_)                                                                       \
            throw "402 Missing " _ATTR_;                                                                               \
        if (!request[_ATTR_].empty() && request[_ATTR_] != SToStr(SToInt(request[_ATTR_])))                            \
            throw "402 Malformed " _ATTR_;                                                                             \
    } while (false)
#define BVERIFY_ATTRIBUTE_INT64(_ATTR_, _MIN_SIZE_)                                                                    \
    do {                                                                                                               \
        if (request[_ATTR_].size() < _MIN_SIZE_)                                                                       \
            throw "402 Missing " _ATTR_;                                                                               \
        if (!request[_ATTR_].empty() && request[_ATTR_] != SToStr(SToInt64(request[_ATTR_])))                          \
            throw "402 Malformed " _ATTR_;                                                                             \
    } while (false)
#define BVERIFY_ATTRIBUTE_SIZE(_ATTR_, _MIN_SIZE_, _MAX_SIZE_)                                                         \
    do {                                                                                                               \
        if (request[_ATTR_].size() < _MIN_SIZE_)                                                                       \
            throw "402 Missing " _ATTR_;                                                                               \
        if (request[_ATTR_].size() > _MAX_SIZE_)                                                                       \
            throw "402 Malformed " _ATTR_;                                                                             \
    } while (false)
#define BVERIFY_ATTRIBUTE_DATE(_ATTR_, _MIN_SIZE_)                                                                     \
    do {                                                                                                               \
        if (request[_ATTR_].size() < _MIN_SIZE_)                                                                       \
            throw "402 Missing " _ATTR_;                                                                               \
        if (!request[_ATTR_].empty() && !SREMatch("^\\d{4}-\\d{2}-\\d{2}( \\d{2}:\\d{2}:\\d{2})?$", request[_ATTR_]))  \
            throw "402 Malformed " _ATTR_;                                                                             \
    } while (false)
#define BTOLOWER_ATTRIBUTE(_ATTR_)                                                                                     \
    do {                                                                                                               \
        if (request.isSet(_ATTR_))                                                                                     \
            request[_ATTR_] = SToLower(request[_ATTR_]);                                                               \
    } while (false)
#define BTOUPPER_ATTRIBUTE(_ATTR_)                                                                                     \
    do {                                                                                                               \
        if (request.isSet(_ATTR_))                                                                                     \
            request[_ATTR_] = SToUpper(request[_ATTR_]);                                                               \
    } while (false)

// We use these sizes to make sure the storage engine does not silently
// truncate data.  We throw an exception instead.  Clearly, if saving JSON, a
// truncation would be a disaster.
// Common max sizes
#define BMAX_SIZE_NONCOLUMN 1024 * 1024 * 1024 // No need to restrict size on these inputs.
#define BMAX_SIZE_QUERY 1024 * 1024            // 1MB seems large enough for a query
#define BMAX_SIZE_BLOB 1024 * 1024             // 1MB seems large enough for a general blob
#define BMAX_SIZE_SMALL 255                    // Most string columns are this.

// BREGISTER_PLUGIN is a macro to auto-instantiate a global instance of a plugin (_CT_). This lets plugin implementors
// just write `BREGISTER_PLUGIN(MyPluginName)` at the bottom of an implementation cpp file and get the plugin
// automatically registered with Bedrock.
//
// Why the weird `EXPAND{N}` macros? Because the pre-processor doesn't do recursive macro expansion with concatenation.
// See: http://stackoverflow.com/questions/1597007/
#define BREGISTER_PLUGIN_EXPAND2(_CT_, _NUM_) _CT_ __BREGISTER_PLUGIN_##_CT_##_NUM_
#define BREGISTER_PLUGIN_EXPAND1(_CT_, _NUM_) BREGISTER_PLUGIN_EXPAND2(_CT_, _NUM_)
#define BREGISTER_PLUGIN(_CT_) BREGISTER_PLUGIN_EXPAND1(_CT_, __COUNTER__)

// BedrockPlugin.h
#endif
