/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    Serializable.h
 * Path:    libstuff/JSON/Serializable.h
 * Pair:    (none; header-only, no .cpp)
 *
 * INTENT
 *   A CRTP base that documents and compile-time-enforces a
 *   toJSON()/fromJSON() contract on a derived type, without a vtable, so
 *   the derived type stays an aggregate (designated initializers keep
 *   working). No consumer in this repo currently derives from it; it
 *   reads as a contract meant for downstream application types.
 *
 * OBJECTS
 *   Serializable<Derived>  - CRTP struct; its toJSON()/fromJSON() are
 *                            static_assert stubs that only fire if the
 *                            derived class fails to hide them via name
 *                            hiding.
 *   always_false_v<T>      - inline constexpr template variable; delays
 *                            the static_assert failure to instantiation
 *                            time (the base method is never actually
 *                            called otherwise).
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; it is a JSON-serialization contract helper, reasonably grouped
 *   with the rest of the JSON package even though the CRTP trick itself
 *   is generic C++, not JSON-specific.
 *
 * NAMING QUALITY
 *   Clear; always_false_v follows the established standard-library-style
 *   naming for this kind of trait helper.
 * ─────────────────────────────────────────────────────────────────────*/

#pragma once

#include "Value.h"

using namespace std;

namespace JSON {
/**
 * Helper for static_assert that depends on a template parameter.
 * We can't use `false` directly in static_assert because it would always fail at template definition time.
 * This trick delays the check until template instantiation.
 */
template<typename T>
inline constexpr bool always_false_v = false;

/**
 * CRTP base class for JSON-serializable types.
 *
 * Usage:
 *   struct MyData : JSON::Serializable<MyData> {
 *       int64_t id = 0;
 *       string name;
 *       JSON::Value toJSON() const;  // ← Must implement this
 *   };
 *
 * Benefits of this approach:
 * - Preserves aggregate status (no virtual functions) → designated initializers work
 * - Zero runtime overhead (no vtable)
 * - Clear compile-time error if toJSON() is not implemented
 * - Documents serialization intent in the type signature
 *
 * How it works:
 * - If Derived defines toJSON(), name hiding means the base version is never called
 * - If Derived forgets toJSON(), calling it triggers the base version's static_assert
 * - The always_false_v trick ensures the assert only fires when actually instantiated
 */
template<typename Derived>
struct Serializable
{
    JSON::Value toJSON() const
    {
        static_assert(always_false_v<Derived>,
            "Derived must define JSON::Value toJSON() const");
    }

    static Derived fromJSON(const JSON::Value&)
    {
        static_assert(always_false_v<Derived>,
            "Derived must define static Derived fromJSON(const JSON::Value&)");
    }
};
}
