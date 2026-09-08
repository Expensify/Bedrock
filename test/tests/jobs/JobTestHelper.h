/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    JobTestHelper.h
 * Path:    test/tests/jobs/JobTestHelper.h
 * Pair:    JobTestHelper.cpp
 *
 * INTENT
 *   Tiny shared helper for the Jobs plugin test suite: parses a
 *   "%Y-%m-%d %H:%M:%S" datetime string (as stored in the jobs table)
 *   back into a time_t so tests can compute time deltas.
 *
 * OBJECTS
 *   JobTestHelper                              - stateless helper class
 *                                                (all-static, no instance data).
 *   JobTestHelper::getTimestampForDateTimeString - parses a datetime string
 *                                                using strptime + mktime.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; scoped to test/tests/jobs and named for its purpose.
 *
 * NAMING QUALITY
 *   Fine; no `S` prefix needed since this is a test-only helper, not a
 *   shared libstuff utility type.
 * ─────────────────────────────────────────────────────────────────────*/

#pragma once

#include <libstuff/libstuff.h>

class JobTestHelper {
public:
    static time_t getTimestampForDateTimeString(const string& datetime);
};
