#include "test/lib/tpunit++.hpp"
#include <ostream>
#include <stdexcept>
#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <sqlitecluster/SQLiteNode.h>
#include <test/lib/BedrockTester.h>

struct SQResultFormatterTest : tpunit::TestFixture {
    SQResultFormatterTest()
        : tpunit::TestFixture("SQResultFormatter",
                              BEFORE_CLASS(SQResultFormatterTest::setup),
                              TEST(SQResultFormatterTest::listNoHeader),
                              AFTER_CLASS(SQResultFormatterTest::tearDown))
    { }

    BedrockTester* tester;

    void setup() {
        tester = new BedrockTester({}, {
            "CREATE TABLE demo_format (id INTEGER, name TEXT, note TEXT, qty INTEGER, price REAL, misc TEXT );",
R"(
INSERT INTO demo_format (id, name, note, qty, price, misc) VALUES
-- 1: simple baseline
(1,  'Bob',                'Simple row',                       10,   19.99, 'ok'),

-- 2: comma in a field
(2,  'Smith, John',        'Comma in name',                     5,     3.50, 'has,comma'),

-- 3: double quotes in a field (CSV should quote & escape)
(3,  'Alice "Ace"',        'Double quotes in name',             7,     2.75, 'He said "Hello"'),

-- 4: newline (LF) inside text
(4,  'Line Feeder',        'line one
line two',                                                     1,     0.99, 'contains LF'),

-- 5: explicit CRLF inside text (use CHAR(10)||CHAR(10))
(5,  'Carriage Return',    'first'||CHAR(10)||'second', 2,   1.25, 'contains LFLF'),

-- 6: tab character inside text (TSV .mode tabs will look messy, on purpose)
(6,  'Tabby',              'has'||CHAR(9)||'tab',               3,     4.00, 'A'||CHAR(9)||'B'),

-- 7: empty string vs NULL in misc
(7,  'Empty/Missing',      'empty string in misc next row is NULL', 0, 0.00, ''),

-- 8: actual NULL value (not the string "NULL")
(8,  'Null Misc',          'misc is NULL here',                 0,     NULL, NULL),

-- 9: scientific notation
(9,  'Sci Notation',       'price uses 1.23e+10',               1,  1.23e+10, 'big number'),

-- 10: negative integer
(10, 'Negative Qty',       'negative quantity',               -42,     5.25, 'neg qty'),

-- 11: very long field to stretch column mode
(11, 'Supercalifragilisticexpialidocious',
'long name to widen columns significantly',                8,    12.34, 'loooooooooooooooooooooooooooooong'),

-- 12: UTF-8 with accent
(12, 'café',               'accented char',                     2,     3.14, 'naïve façade'),

-- 13: UTF-8 emoji
(13, 'Smiley 🙂',          'emoji inside text',                 1,     0.10, 'rocket 🚀'),

-- 14: commas + quotes in the same field
(14, 'Quoter',             'He said, "Hello, world", then left.', 4,   6.00, 'mix, "both"'),

-- 15: literal text "NULL" (not SQL NULL)
(15, 'Literal NULL',       'This string is "NULL"',             9,     9.99, 'NULL'),

-- 16: pipe character (|) — useful for default column/list separators
(16, 'Piper|Piped',        'contains | pipe',                   6,     2.22, 'A|B|C'),

-- 17: leading/trailing spaces preserved
(17, '  spaced  ',         '  keep spaces  ',                   5,     1.11, '  around'),

-- 18: zero qty and empty note
(18, 'Zero',               '',                                  0,     0.00, 'empty note'),

-- 19: tabs and commas together
(19, 'Mix\t,Match',        'tab'||CHAR(9)||'and,comma',         3,     7.77, 'both'||CHAR(9)||',present'),

-- 20: tricky combo (quotes, comma, newline, tab, LF)
(20, 'Tricky "Case", Inc.', 'start'||CHAR(9)||'mid, "q"'||CHAR(10)||'end',
                                                    12, 123.456789, 'final'||CHAR(9)||'val,ue');
)",
        });
    }

    void tearDown() {
        delete tester;
    }

    void listNoHeader() {
        SData query("Query");
        query["query"] = "SELECT * FROM demo_format;";
        auto result = tester->executeWaitMultipleData({query});
        string expected =
R"(1|Bob|Simple row|10|19.99|ok
2|Smith, John|Comma in name|5|3.5|has,comma
3|Alice "Ace"|Double quotes in name|7|2.75|He said "Hello"
4|Line Feeder|line one
line two|1|0.99|contains LF
5|Carriage Return|first
second|2|1.25|contains LFLF
6|Tabby|has	tab|3|4.0|A	B
7|Empty/Missing|empty string in misc next row is NULL|0|0.0|
8|Null Misc|misc is NULL here|0||
9|Sci Notation|price uses 1.23e+10|1|12300000000.0|big number
10|Negative Qty|negative quantity|-42|5.25|neg qty
11|Supercalifragilisticexpialidocious|long name to widen columns significantly|8|12.34|loooooooooooooooooooooooooooooong
12|café|accented char|2|3.14|naïve façade
13|Smiley 🙂|emoji inside text|1|0.1|rocket 🚀
14|Quoter|He said, "Hello, world", then left.|4|6.0|mix, "both"
15|Literal NULL|This string is "NULL"|9|9.99|NULL
16|Piper|Piped|contains | pipe|6|2.22|A|B|C
17|  spaced  |  keep spaces  |5|1.11|  around
18|Zero||0|0.0|empty note
19|Mix\t,Match|tab	and,comma|3|7.77|both	,present
20|Tricky "Case", Inc.|start	mid, "q"
end|12|123.456789|final	val,ue)";

        if (result[0].content != expected) {
            cout << "ResultSize: " << result[0].content.size() << endl;
            cout << "ExpectedSize: " << expected.size() << endl;

            for (size_t i = 0; i < min(result[0].content.size(), expected.size()); i++) {
                if (result[0].content[i] != expected[i]) {
                    cout << "Difference at character " << i << " expected: '" << (int)expected[i] << "', got '" << (int)result[0].content[i] << "'." << endl;
                    cout << expected.substr(i - 10, 20) << endl;
                    break;
                }
            }
        }
        ASSERT_EQUAL(result[0].content, expected);
    }
} __SQResultFormatterTest;
