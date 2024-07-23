#include "SQuery.h"

string SQuery::prepare(const string& query, const map<string, QuerySerializable>& params)
{
    string preparedQuery = query;
    for (const auto& param: params) {
        const string placeholder = "{" + param.first + "}";
        string replacement;

        // Use std::visit to call the correct overload of SQ
        std::visit([&replacement](auto&& arg) {
            replacement = SQ(arg);
        }, param.second);

        size_t pos = 0;
        while ((pos = preparedQuery.find(placeholder, pos)) != std::string::npos) {
            preparedQuery.replace(pos, placeholder.length(), replacement);
            pos += replacement.length();
        }
    }
    return preparedQuery;
}
