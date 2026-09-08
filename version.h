/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    version.h
 * Path:    version.h
 *
 * INTENT
 *   Defines a fallback SVERSION macro (a placeholder 40-character hash);
 *   unlike bedrockVersion.h's VERSION, this macro is not referenced by
 *   any other file in the repo.
 *
 * OBJECTS
 *   SVERSION - placeholder all-zero revision-string macro.
 *
 * OUT OF PLACE
 *   [CANDIDATE] SVERSION is never included or referenced anywhere else
 *   in the repo -- appears to be dead code superseded by bedrockVersion.h's
 *   VERSION macro.
 *
 * NAME/LOCATION FIT
 *   Name collides conceptually with bedrockVersion.h's VERSION; having
 *   both invites picking the wrong one.
 *
 * NAMING QUALITY
 *   Carries the repo's `S` shared-utility prefix though it is an unused
 *   macro, not a type -- misleading given that convention is normally
 *   reserved for libstuff types.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
#ifndef SVERSION
#define SVERSION "0000000000000000000000000000000000000000"
#endif
