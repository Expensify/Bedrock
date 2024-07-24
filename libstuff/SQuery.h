#pragma once

#include "libstuff.h"

#include <string>
#include <variant>

using namespace std;

class SQuery {
public:
    using QuerySerializable = variant<const char*, string, int, unsigned, uint64_t, int64_t, double>;
    static string prepare(const string& query, const map<string, QuerySerializable>& params);
};
