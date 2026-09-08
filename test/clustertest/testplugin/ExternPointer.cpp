/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    ExternPointer.cpp
 * Path:    test/clustertest/testplugin/ExternPointer.cpp
 * Pair:    (declared extern in test/clustertest/testplugin/TestPlugin.cpp)
 *
 * INTENT
 *   Defines a single global, uninitialized int pointer whose only purpose
 *   is to be dereferenced out of bounds by TestPlugin's
 *   generatesegfaultpeek/generatesegfaultprocess commands, giving
 *   clustertest a reliable way to crash the server on purpose.
 *
 * OBJECTS
 *   __pointerToFakeIntArray - global int pointer, deliberately never
 *       allocated; reading through it triggers a real segfault.
 *
 * OUT OF PLACE
 *   [CANDIDATE] Being its own translation unit for a single global is
 *   unusual; the comment says it exists so BadCommandTest has "something
 *   weird" to reference, but nothing else in this batch is named
 *   BadCommandTest, so its actual consumer(s) live elsewhere.
 *
 * NAME/LOCATION FIT
 *   Directory fits (test-only plugin support); filename describes the
 *   mechanism (an extern pointer) rather than the intent (crash trigger),
 *   which is a minor mismatch.
 *
 * NAMING QUALITY
 *   Reserved double-underscore prefix (`__pointerToFakeIntArray`) is
 *   technically undefined behavior at the language level (identifiers
 *   with `__` are reserved to the implementation); the name itself is
 *   otherwise clear about its fictitious purpose.
 * ─────────────────────────────────────────────────────────────────────*/
// Literaly just exists for BadCommandTest to need to reference something weird.
int* __pointerToFakeIntArray;
