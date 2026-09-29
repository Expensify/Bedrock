#pragma once

#include <iostream>
#include <string>
#include <set>
#include <map>
#include <list>
#include <optional>
#include <sstream>

using namespace std;

namespace tpunit {void tpunit_report_comparison(const string& lhs, const string& rhs, bool isEqual);}

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
        ostringstream lhs, rhs;
        lhs << a;
        rhs << b;
        tpunit::tpunit_report_comparison(lhs.str(), rhs.str(), isEqual);
    }
};
