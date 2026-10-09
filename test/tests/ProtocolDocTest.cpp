#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <test/lib/BedrockTester.h>

// Parses every example message in docs/protocol.md with the same parser Bedrock uses, so that an
// example which no longer reflects the protocol fails the build.
//
// Example blocks in the document are fenced with an info string that says how to rebuild the body
// from the markdown. Markdown cannot represent a trailing newline or a CRLF inside a fence, so the
// info string supplies what the text cannot:
//
//   ```bwp        body uses LF, and does not end with a newline
//   ```bwp-lf     body uses LF, and ends with one
//   ```bwp-crlf   body is itself one or more BWP messages, so it uses CRLF, and does not end with a newline
//
// Field lines are always rebuilt with CRLF, whatever the document shows. Line-ending tolerance is
// covered by LibStuffTest and FastHTTPParsing; this fixture only checks that each documented
// message is well formed and that its Content-Length matches its body.
struct ProtocolDocTest : tpunit::TestFixture
{
    ProtocolDocTest() : tpunit::TestFixture(true, "ProtocolDocTest",
                                            TEST(ProtocolDocTest::examplesParse))
    {
    }

    struct Example {
        size_t line;
        string tag;
        string message;
    };

    // The unit test binary runs from either the repository root or from `test`.
    string loadDoc()
    {
        for (const string& path : {"docs/protocol.md"s, "../docs/protocol.md"s}) {
            string contents;
            if (SFileLoad(path, contents) && contents.size()) {
                return contents;
            }
        }
        return "";
    }

    // SParseList discards empty components, so it cannot be used here: a blank line is what
    // separates the fields from the body.
    vector<string> splitLines(const string& text)
    {
        vector<string> lines;
        size_t start = 0;
        while (start <= text.size()) {
            size_t end = text.find('\n', start);
            if (end == string::npos) {
                lines.push_back(text.substr(start));
                break;
            }
            lines.push_back(text.substr(start, end - start));
            start = end + 1;
        }
        return lines;
    }

    vector<Example> extractExamples(const string& doc)
    {
        vector<Example> examples;
        string tag;
        size_t openedAt = 0;
        vector<string> block;

        size_t lineNumber = 0;
        for (const string& line : splitLines(doc)) {
            lineNumber++;
            if (tag.empty()) {
                if (SStartsWith(line, "```bwp")) {
                    tag = line.substr(3);
                    openedAt = lineNumber;
                    block.clear();
                }
                continue;
            }
            if (line == "```") {
                examples.push_back({openedAt, tag, rebuild(tag, block)});
                tag.clear();
                continue;
            }
            block.push_back(line);
        }
        return examples;
    }

    // Turn the lines of one fenced block back into the octets the document says they represent.
    string rebuild(const string& tag, const vector<string>& block)
    {
        // Everything up to the first empty line is the method line and the fields.
        vector<string> header;
        vector<string> body;
        bool inBody = false;
        for (const string& line : block) {
            if (!inBody && line.empty()) {
                inBody = true;
                continue;
            }
            (inBody ? body : header).push_back(line);
        }

        string message;
        for (const string& line : header) {
            message += line + "\r\n";
        }
        message += "\r\n";

        const string bodySeparator = (tag == "bwp-crlf") ? "\r\n" : "\n";
        for (size_t i = 0; i < body.size(); i++) {
            if (i) {
                message += bodySeparator;
            }
            message += body[i];
        }
        if (tag == "bwp-lf" && body.size()) {
            message += "\n";
        }
        return message;
    }

    void examplesParse()
    {
        const string doc = loadDoc();
        ASSERT_TRUE(doc.size());

        vector<Example> examples = extractExamples(doc);

        // Guard against the extractor silently matching nothing, which would let this fixture pass
        // while checking no examples at all.
        ASSERT_GREATER_THAN(examples.size(), 10u);

        for (const Example& example : examples) {
            const string where = "docs/protocol.md:" + to_string(example.line) + " (```" + example.tag + ")";

            string methodLine;
            STable headers;
            string content;
            const size_t consumed = SParseHTTP(example.message.c_str(), example.message.size(), methodLine, headers, content);

            // A zero return means the parser wanted more octets, so Content-Length claims a longer
            // body than the example shows.
            if (!consumed) {
                TESTINFO(where + ": did not parse. Content-Length is larger than the body.");
            }
            ASSERT_TRUE(consumed);

            // Unconsumed octets mean the parser stopped early, so Content-Length claims a shorter
            // body than the example shows.
            if (consumed != example.message.size()) {
                TESTINFO(where + ": parsed " + to_string(consumed) + " of " + to_string(example.message.size())
                         + " octets. Content-Length is smaller than the body.");
            }
            ASSERT_EQUAL(consumed, example.message.size());

            if (!methodLine.size()) {
                TESTINFO(where + ": no method line.");
            }
            ASSERT_TRUE(methodLine.size());

            // The two checks above already imply this for a well formed message. Checking it
            // directly names the failure as the number a reader would edit.
            auto it = headers.find("Content-Length");
            if (it != headers.end()) {
                if (SToUInt64(it->second) != content.size()) {
                    TESTINFO(where + ": Content-Length says " + it->second + " but the body is "
                             + to_string(content.size()) + " octets.");
                }
                ASSERT_EQUAL(SToUInt64(it->second), content.size());
            }
        }
    }
} __ProtocolDocTest;

// vim: set expandtab shiftwidth=4 tabstop=4:
