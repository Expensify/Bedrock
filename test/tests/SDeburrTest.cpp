#include <libstuff/SDeburr.h>
#include <test/lib/tpunit++.hpp>
#include <string>

using namespace std;

struct SDeburrTest : tpunit::TestFixture {
    SDeburrTest() : tpunit::TestFixture("SDeburr",
                                         TEST(SDeburrTest::testBasicASCII),
                                         TEST(SDeburrTest::testDiacritics),
                                         TEST(SDeburrTest::testMixed),
                                         TEST(SDeburrTest::testCombining)) {}

    void testBasicASCII() {
        ASSERT_EQUAL(SDeburr::deburr("Fabio"), string("fabio"));
        ASSERT_EQUAL(SDeburr::deburr("WARIO"), string("wario"));
        ASSERT_EQUAL(SDeburr::deburr("#Pizza"), string("#pizza"));
    }

    void testDiacritics() {
        ASSERT_EQUAL(SDeburr::deburr("Fábio"), string("fabio"));
        ASSERT_EQUAL(SDeburr::deburr("Wário"), string("wario"));
        ASSERT_EQUAL(SDeburr::deburr("Wálüîgi"), string("waluigi"));
        ASSERT_EQUAL(SDeburr::deburr("Ångström"), string("angstrom"));
        ASSERT_EQUAL(SDeburr::deburr("façade"), string("facade"));
        ASSERT_EQUAL(SDeburr::deburr("straße"), string("strasse"));
        ASSERT_EQUAL(SDeburr::deburr("Æther"), string("aether"));
        ASSERT_EQUAL(SDeburr::deburr("Œuvre"), string("oeuvre"));
    }

    void testMixed() {
        ASSERT_EQUAL(SDeburr::deburr(" Café #Team"), string(" cafe #team"));
        ASSERT_EQUAL(SDeburr::deburr("L'ESPRIT"), string("l'esprit"));
    }

    void testCombining() {
        // "a" + combining acute (U+0301)
        ASSERT_EQUAL(SDeburr::deburr(string("a\xCC\x81")), string("a"));
    }
} __SDeburrTest;


