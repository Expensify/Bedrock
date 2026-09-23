#include <libstuff/SDeburr.h>
#include <test/lib/tpunit++.hpp>
#include <string>

using namespace std;

struct SDeburrTest : tpunit::TestFixture
{
    SDeburrTest() : tpunit::TestFixture("SDeburr",
                                        TEST(SDeburrTest::testBasicASCII),
                                        TEST(SDeburrTest::testDiacritics),
                                        TEST(SDeburrTest::testMixed),
                                        TEST(SDeburrTest::testCombining),
                                        TEST(SDeburrTest::testKitchenSink),
                                        TEST(SDeburrTest::testTurkish),
                                        TEST(SDeburrTest::testPolishSlavic),
                                        TEST(SDeburrTest::testNordic),
                                         TEST(SDeburrTest::testEmojiAndCJK),
                                         TEST(SDeburrTest::testEmbeddedNUL))
    {
    }

    void testBasicASCII()
    {
        ASSERT_EQUAL(SDeburr::deburr("Fabio"), string("Fabio"));
        ASSERT_EQUAL(SDeburr::deburr("WARIO"), string("WARIO"));
        ASSERT_EQUAL(SDeburr::deburr("#Pizza"), string("#Pizza"));
    }

    void testDiacritics()
    {
        ASSERT_EQUAL(SDeburr::deburr("Fábio"), string("Fabio"));
        ASSERT_EQUAL(SDeburr::deburr("Wário"), string("Wario"));
        ASSERT_EQUAL(SDeburr::deburr("Wálüîgi"), string("Waluigi"));
        ASSERT_EQUAL(SDeburr::deburr("Ångström"), string("Angstrom"));
        ASSERT_EQUAL(SDeburr::deburr("façade"), string("facade"));
        ASSERT_EQUAL(SDeburr::deburr("straße"), string("strasse"));
        ASSERT_EQUAL(SDeburr::deburr("Æther"), string("AEther"));
        ASSERT_EQUAL(SDeburr::deburr("Œuvre"), string("OEuvre"));
    }

    void testMixed()
    {
        ASSERT_EQUAL(SDeburr::deburr(" Café #Team"), string(" Cafe #Team"));
        ASSERT_EQUAL(SDeburr::deburr("L'ESPRIT"), string("L'ESPRIT"));
    }

    void testCombining()
    {
        // "a" + combining acute (U+0301)
        ASSERT_EQUAL(SDeburr::deburr(string("a\xCC\x81")), string("a"));
    }

    void testKitchenSink()
    {
        ASSERT_EQUAL(SDeburr::deburr("Crème Brûlée déjà vu – São Paulo smörgåsbord"),
                     string("Creme Brulee deja vu – Sao Paulo smorgasbord"));
    }

    void testTurkish()
    {
        ASSERT_EQUAL(SDeburr::deburr("İstanbul"), string("Istanbul"));
        ASSERT_EQUAL(SDeburr::deburr("Iı"), string("Ii"));
    }

    void testPolishSlavic()
    {
        ASSERT_EQUAL(SDeburr::deburr("Łódź"), string("Lodz"));
        ASSERT_EQUAL(SDeburr::deburr("Śródka"), string("Srodka"));
        ASSERT_EQUAL(SDeburr::deburr("Żubr"), string("Zubr"));
        ASSERT_EQUAL(SDeburr::deburr("Źrebak"), string("Zrebak"));
    }

    void testNordic()
    {
        ASSERT_EQUAL(SDeburr::deburr("Þingvellir"), string("THingvellir"));
        ASSERT_EQUAL(SDeburr::deburr("Æsir"), string("AEsir"));
        ASSERT_EQUAL(SDeburr::deburr("Œuvre"), string("OEuvre"));
        ASSERT_EQUAL(SDeburr::deburr("Ångström"), string("Angstrom"));
    }

    void testEmojiAndCJK()
    {
        ASSERT_EQUAL(SDeburr::deburr("pizza 🍕"), string("pizza 🍕"));
        ASSERT_EQUAL(SDeburr::deburr("東京"), string("東京"));
    }

    void testEmbeddedNUL()
    {
        // The string overload must process the explicit length, including NUL bytes.
        ASSERT_EQUAL(SDeburr::deburr(string("\0B", 2)), string("\0B", 2));
        ASSERT_EQUAL(SDeburr::deburr(string("A\0B", 3)), string("A\0B", 3));
        ASSERT_EQUAL(SDeburr::deburr(string("AB\0", 3)), string("AB\0", 3));
        ASSERT_EQUAL(SDeburr::deburr(string("A\0\0B", 4)), string("A\0\0B", 4));

        // Accented text after a NUL is still transliterated. é is the two-byte sequence C3 A9.
        const string accentedAfterNul = string("A\0", 2) + "é";
        ASSERT_EQUAL(SDeburr::deburr(accentedAfterNul), string("A\0e", 3));

        // The NUL-terminated pointer overload still stops at the first NUL.
        const unsigned char truncated[] = {'A', '\0', 'B'};
        ASSERT_EQUAL(SDeburr::deburr(truncated), string("A"));

        // Ordinary transliteration is unchanged.
        ASSERT_EQUAL(SDeburr::deburr("Fábio"), string("Fabio"));
    }
} __SDeburrTest;
