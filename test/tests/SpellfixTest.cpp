#include <test/lib/BedrockTester.h>

struct SpellfixTest : tpunit::TestFixture {
    SpellfixTest()
        : tpunit::TestFixture(BEFORE_CLASS(SpellfixTest::setup),
                              TEST(SpellfixTest::test),
                              AFTER_CLASS(SpellfixTest::tearDown)) {
        NAME(Read);
    }

    BedrockTester* tester;

    void setup() { tester = new BedrockTester(); }

    void tearDown() { delete tester; }

    void test() {
        SData status("Query");
        status["query"] = "CREATE VIRTUAL TABLE demo USING spellfix1;";
        string response = tester->executeWait(status);

        vector<string> commonWords = {
            "the", "be", "to", "of", "and", "a", "in", "that", "have", "I", "it",
            "for", "not", "on", "with", "he", "as", "you", "do", "at", "this", "but",
            "his", "by", "from", "they", "we", "say", "her", "she", "or", "an", "will",
            "my", "one", "all", "would", "there", "their", "what", "so", "up", "out", "if",
            "about", "who", "get", "which", "go", "me", "when", "make", "can", "like", "time",
            "no", "just", "him", "know", "take", "people", "into", "year", "your", "good",
            "some", "could", "them", "see", "other", "than", "then", "now", "look", "only",
            "come", "its", "over", "think", "also", "back", "after", "use", "two", "how",
            "our", "work", "first", "well", "way", "even", "new", "want", "because", "any",
            "these", "give", "day", "most", "us"
        };

        for (auto i = 99; i >= 0; i--) {
            status["query"] = "INSERT INTO demo(word) SELECT " + SQ(commonWords[i]) + " AS word;";
            string response = tester->executeWait(status);
        }

        status["query"] = "SELECT word FROM demo WHERE word MATCH 'bi';";
        response = tester->executeWait(status);

        ASSERT_TRUE(SContains(response, "by"));
        ASSERT_TRUE(SContains(response, "be"));
        ASSERT_TRUE(SContains(response, "but"));
    }

} __SpellfixTest;
