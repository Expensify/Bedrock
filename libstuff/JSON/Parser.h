#pragma once

#include <memory>
#include <string>

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
    static unique_ptr<Value> read(const string& json);

    /**
     * Parse a complete JSON document with UTF-8 validation and duplicate-key
     * rejection. Unlike read(), this method is length-aware and rejects embedded
     * NUL bytes.
     *
     * @throws JSON::InvalidArgument
     */
    static unique_ptr<Value> readStrict(const string& json);

    /**
     * Read and parse a json string but don't throw an exception if the
     * JSON is invalid. Accept whatever value happens to come out.
     *
     * @param json The json string
     * @return a JSON::Value with the contents
     */
    static unique_ptr<Value> readUnsafe(const string& json);
};
}
