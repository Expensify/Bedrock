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
        ASSERT_EQUAL(SDeburr::deburrToASCII("Fabio"), string("fabio"));
        ASSERT_EQUAL(SDeburr::deburrToASCII("WARIO"), string("wario"));
        ASSERT_EQUAL(SDeburr::deburrToASCII("#Pizza"), string("#pizza"));
    }

    void testDiacritics() {
        ASSERT_EQUAL(SDeburr::deburrToASCII("Fábio"), string("fabio"));
        ASSERT_EQUAL(SDeburr::deburrToASCII("Wário"), string("wario"));
        ASSERT_EQUAL(SDeburr::deburrToASCII("Wálüîgi"), string("waluigi"));
        ASSERT_EQUAL(SDeburr::deburrToASCII("Ångström"), string("angstrom"));
        ASSERT_EQUAL(SDeburr::deburrToASCII("façade"), string("facade"));
        ASSERT_EQUAL(SDeburr::deburrToASCII("straße"), string("strasse"));
        ASSERT_EQUAL(SDeburr::deburrToASCII("Æther"), string("aether"));
        ASSERT_EQUAL(SDeburr::deburrToASCII("Œuvre"), string("oeuvre"));
    }

    void testMixed() {
        ASSERT_EQUAL(SDeburr::deburrToASCII(" Café #Team"), string(" cafe #team"));
        ASSERT_EQUAL(SDeburr::deburrToASCII("L'ESPRIT"), string("l'esprit"));
    }

    void testCombining() {
        // "a" + combining acute (U+0301)
        ASSERT_EQUAL(SDeburr::deburrToASCII(string("a\xCC\x81")), string("a"));
    }
} __SDeburrTest;


