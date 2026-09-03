#pragma once
#include <test/lib/tpunit++.hpp>
#include <libstuff/JSON/Value.h>
#include <string>

class JSONUtilsTest : public tpunit::TestFixture
{
public:
    JSONUtilsTest();

    void mergeObjectsOverwritesRightOrder();
    void mergeObjectsHandlesLeftNonObject();
    void mergeObjectsHandlesRightNonObject();
    void mergeObjectsBothNonObjectsToEmptyObject();
    void mergeDeepWithSQLiteBehavior();
    void applyJSONMergePatchNonObjectPatchReplaces();
    void applyJSONMergePatchNonObjectExistingTreatedAsEmptyObject();
    void mergeObjectJSON();
    void stripOutFields();
    void removeObjectKeysWithNullValues();
    void containAnyKeys();
    void getFirstString();
    void parseOrDefault();
    void sanitizeJSONStringForTransportStripsControlBytes();
    void sanitizeJSONStringForTransportPreservesValidUTF8();
    void sanitizeJSONStringForTransportRejectsInvalidMultiByte();
    void parseJSONPath();

private:
    string getOldObject();
    string getNewObject();
    void assertObjectMerged(const JSON::Value& object);
};
