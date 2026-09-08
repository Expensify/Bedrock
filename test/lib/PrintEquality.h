/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    PrintEquality.h
 * Path:    test/lib/PrintEquality.h
 *
 * INTENT
 *   Backs the tpunit++ ASSERT_EQUAL/ASSERT_NOT_EQUAL family: on a failed
 *   comparison, its constructor prints both compared values with an "="
 *   or "!=" between them so a failing assertion's actual values show up in
 *   the test output.
 *
 * OBJECTS
 *   PrintEquality - constructor-only helper; templated on the two compared types, prints
 *                   `a (= or !=) b` to cout when constructed.
 *   operator<<(ostream&, const list<T>&)      - free function; prints a list via SComposeList.
 *   operator<<(ostream&, const set<T>&)       - free function; prints a set via SComposeList.
 *   operator<<(ostream&, const map<T,U>&)     - free function; prints a map as one "k: v" line per entry.
 *   operator<<(ostream&, const optional<T>&)  - free function; prints the contained value or "(nullopt)".
 *
 * OUT OF PLACE
 *   [CANDIDATE] The four `operator<<` overloads for list/set/map/optional are generic stream-
 *   formatting utilities with no dependency on PrintEquality or tpunit, bundled here only because
 *   PrintEquality's own printing happens to need them. A shared stream-formatting header (e.g. in
 *   libstuff) would let non-test code use the same overloads instead of only test code that
 *   happens to include this file.
 *
 * NAME/LOCATION FIT
 *   Partial fit: the file does far more than print equality - it defines general-purpose
 *   container-printing operators the class itself merely calls into.
 *
 * NAMING QUALITY
 *   PrintEquality's name matches its own narrow job; the operator<< overloads it drags in are
 *   unnamed (operator overloads) so the naming question doesn't really apply to them.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once

#include <iostream>
#include <string>
#include <set>
#include <map>
#include <list>
#include <optional>

using namespace std;

namespace tpunit {void tpunit_break_check_line();}

template<typename T>
ostream& operator<<(ostream& output, const list<T>& val)
{
    return output << "[" << SComposeList(val) << "]";
}

template<typename T>
ostream& operator<<(ostream& output, const set<T>& val)
{
    return output << "[" << SComposeList(val) << "]";
}

template<typename T, typename U>
ostream& operator<<(ostream& output, const map<T, U>& val)
{
    output << "[Map] {" << endl;
    for (const auto& [k, v] : val) {
        output << k << ": " << v << endl;
    }
    return output << "}";
}

template<typename T>
ostream& operator<<(ostream& output, const optional<T>& val)
{
    if (val.has_value()) {
        return output << val.value();
    }

    return output << "(nullopt)";
}

class PrintEquality {
public:
    template<typename U, typename V>
    PrintEquality(const U& a, const V& b, bool isEqual)
    {
        tpunit::tpunit_break_check_line();
        cout << a << " " << (isEqual ? "=" : "!") << "= " << b << "\n";
    }
};
