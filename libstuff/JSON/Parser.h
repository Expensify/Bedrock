#pragma once

#include <memory>
#include <string_view>

#include "Value.h"

namespace JSON
{
class Parser
{
public:

    /**
     * Read and parse a json string
     *
     * @param json The json string
     * @return a JSON::Value with the contents
     * @throws JSON::InvalidArgument
     */
    static unique_ptr<Value> read(string_view json);

    /**
     * Read and parse a json string but don't throw an exception if the
     * JSON is invalid. Accept whatever value happens to come out.
     *
     * @param json The json string
     * @return a JSON::Value with the contents
     */
    static unique_ptr<Value> readUnsafe(string_view json);
};
}
