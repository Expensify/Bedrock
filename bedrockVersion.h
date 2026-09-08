/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    bedrockVersion.h
 * Path:    bedrockVersion.h
 *
 * INTENT
 *   Defines the VERSION macro reporting the running build's git revision
 *   (from GIT_REVISION, passed in by the Makefile), or a placeholder if
 *   none was supplied.
 *
 * OBJECTS
 *   VERSION - build-revision string macro, consumed by main.cpp (--version)
 *       and BedrockServer (its _version field).
 *   STRINGIZE/EXPAND_STRING - private stringizing helpers used only to
 *       expand GIT_REVISION into VERSION.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   Name and purpose closely shadow version.h's SVERSION; the two are
 *   easy to confuse despite one (VERSION) being live and the other
 *   (SVERSION) unreferenced elsewhere in the repo.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
// If the environment sets GIT_REVISION, we'll use that value, otherwise we'll default to a bunch of zeros.
#ifdef VERSION
#error VERSION already defined
#endif
#ifdef GIT_REVISION
#define STRINGIZE(x) #x
#define EXPAND_STRING(x) STRINGIZE(x)
#define VERSION EXPAND_STRING(GIT_REVISION)
#else
#define VERSION "0000000"
#endif
