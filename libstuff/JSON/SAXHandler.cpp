#include "SAXHandler.h"

using namespace JSON;

bool SAXHandler::Null()
{
    endContext();
    return true;
}

bool SAXHandler::Bool(bool b)
{
    Value* context = contexts.back();
    context->valueType = BOOL;
    context->boolValue = b;

    endContext();
    return true;
}

bool SAXHandler::Int(int i)
{
    Value* context = contexts.back();
    context->valueType = INT;
    context->intValue = i;
    context->usingUnsigned = false;

    endContext();
    return true;
}

bool SAXHandler::Uint(unsigned i)
{
    Value* context = contexts.back();
    context->valueType = INT;
    context->intValue = i;
    context->usingUnsigned = false;

    endContext();
    return true;
}

bool SAXHandler::Int64(int64_t i)
{
    Value* context = contexts.back();
    context->valueType = INT;
    context->intValue = i;
    context->usingUnsigned = false;

    endContext();
    return true;
}

bool SAXHandler::Uint64(uint64_t i)
{
    Value* context = contexts.back();
    context->valueType = INT;
    if (i > INT64_MAX) {
        context->uintValue = i;
        context->usingUnsigned = true;
    } else {
        context->intValue = i;
        context->usingUnsigned = false;
    }

    endContext();
    return true;
}

bool SAXHandler::Double(double d)
{
    Value* context = contexts.back();
    context->valueType = FLOAT;
    context->floatValue = d;

    endContext();
    return true;
}

bool SAXHandler::String(const char* str, size_t length, bool copy)
{
    Value* context = contexts.back();
    context->valueType = STRING;
    context->stringValue = string(str, length);

    endContext();
    return true;
}

bool SAXHandler::StartObject()
{
    Value* object = contexts.back();
    object->valueType = OBJECT;
    object->objectValue = make_shared<map<string, Value>>();
    return true;
}

bool SAXHandler::Key(const char* str, size_t length, bool copy)
{
    Value* object = contexts.back();
    const string key(str, length);
    Value& child = (*object->objectValue)[key];
    child = Value();

    // insert new member and set up context
    contexts.push_back(&child);
    return true;
}

bool SAXHandler::EndObject(size_t memberCount)
{
    endContext();
    return true;
}

bool SAXHandler::StartArray()
{
    // We make the placeholder element an array, but we won't be inserting stuff in it until we are done parsing this array
    Value* array = contexts.back();
    array->valueType = ARRAY;
    array->arrayValue = make_shared<vector<Value>>();

    // push space for first element
    list<JSON::Value> arrayContent;
    arrayContent.push_back(Value());

    // push the temporary list so we can retrieve it each time we finish a child element in endContext, and when we finish the array in EndArray
    tempArrayContentLists.push_back(move(arrayContent));

    // set up context for new element
    contexts.push_back(&tempArrayContentLists.back().back());

    return true;
}

bool SAXHandler::EndArray(size_t elementCount)
{
    contexts.pop_back(); // pop unused, empty element
    Value* array = contexts.back();

    // We need to move the temporary list into the array now that it's complete
    list<JSON::Value>& tempArrayContent = tempArrayContentLists.back();
    tempArrayContent.pop_back(); // discard unused last element
    array->arrayReserve(tempArrayContent.size());
    move(begin(tempArrayContent), end(tempArrayContent), back_inserter(*array->arrayValue));
    tempArrayContentLists.pop_back();

    endContext();
    return true;
}

void SAXHandler::endContext()
{
    contexts.pop_back();
    if (!contexts.empty()) {
        Value* parent = contexts.back();
        if (parent->valueType == ARRAY) {
            // We are working within an array, so we need to push the next placeholder element in the list containing the temporary array content
            list<JSON::Value>& tempArrayContent = tempArrayContentLists.back();
            tempArrayContent.push_back(Value());

            // set up context for new element
            contexts.push_back(&tempArrayContent.back());
        }
    }
}

unique_ptr<Value> SAXHandler::getValue()
{
    if (ownershipGiven) {
        throw logic_error("Can't get two copies of a SAXHandler's value");
    }
    ownershipGiven = true;
    return unique_ptr<Value>(root);
};

SAXHandler::SAXHandler() : contexts(), root(), ownershipGiven(false)
{
    root = new Value();
    contexts.push_back(root);
};

bool SAXHandler::RawNumber(const char* str, rapidjson::SizeType len, bool copy)
{
    // Not supported.
    return false;
}

SAXHandler::~SAXHandler()
{
    if (!ownershipGiven) {
        delete root;
    }
}
