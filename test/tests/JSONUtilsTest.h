/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    JSONUtilsTest.h
 * Path:    test/tests/JSONUtilsTest.h
 * Pair:    JSONUtilsTest.cpp
 *
 * INTENT
 *   Declares the tpunit fixture that unit-tests JSON::Utils's merge,
 *   merge-patch, field-stripping, key-search, string-extraction, and
 *   JSON-path functions (JSONUtilsTest.cpp implements it).
 *
 * OBJECTS
 *   JSONUtilsTest : tpunit::TestFixture - declares 17 public test
 *       methods, grouped:
 *       mergeObjectsOverwritesRightOrder/mergeObjectsHandlesLeftNonObject/
 *           mergeObjectsHandlesRightNonObject/
 *           mergeObjectsBothNonObjectsToEmptyObject - mergeDeep() edge
 *           cases with non-object operands.
 *       mergeDeepWithSQLiteBehavior - the useSQLiteMergeBehavior flag's
 *           effect on null keys (delete vs. preserve) and arrays
 *           (replace vs. concatenate).
 *       applyJSONMergePatchNonObjectPatchReplaces/
 *           applyJSONMergePatchNonObjectExistingTreatedAsEmptyObject -
 *           RFC 7396 merge-patch semantics matching SQLite's JSON_PATCH.
 *       mergeObjectJSON - a full deep-merge scenario across nested
 *           objects, arrays, and strings.
 *       stripOutFields/removeObjectKeysWithNullValues/containAnyKeys -
 *           recursive key removal and key-presence search.
 *       getFirstString - scalar-or-first-array-element string
 *           extraction.
 *       parseOrDefault - fallback value on parse failure.
 *       sanitizeJSONStringForTransportStripsControlBytes/
 *           sanitizeJSONStringForTransportPreservesValidUTF8/
 *           sanitizeJSONStringForTransportRejectsInvalidMultiByte -
 *           control-byte stripping and UTF-8 validation.
 *       parseJSONPath - dotted/quoted JSON-path segment splitting.
 *   JSONUtilsTest::getOldObject/getNewObject - private, return literal
 *       JSON fixtures shared by mergeObjectJSON.
 *   JSONUtilsTest::assertObjectMerged - private, shared post-merge
 *       assertions for mergeObjectJSON.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Consistent, descriptive scenario names; fits repo test convention.
 * ─────────────────────────────────────────────────────────────────────*/
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
