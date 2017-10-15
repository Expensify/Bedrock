#include "libstuff.h"
#include "SXML.h"

// --------------------------------------------------------------------------
string SXML::getElementByType(const string& haystack, const string& type)
{
    list<string> elementList;
    if (getElementsByType(haystack, type, elementList)) {
        return elementList.front();
    } else {
        return "";
    }
}

// --------------------------------------------------------------------------
string SXML::getAttribute(const string& element, const string& name)
{
    // Convert to lowercase for searching
    const string& lElement = SToLower(element);
    const string& lName    = SToLower(name);

    // Look for the named attribute
    size_t namePos = lElement.find(lName + "=");
    if (namePos == string::npos) {
        return ""; // Couldn't find the attribute
    }

    size_t pos = namePos + name.size() + 1;
    if (pos >= element.size()) {
        return ""; // No room for a value
    }

    // Look for the end of the value.
    size_t endPos = 0;
    if (element[pos] == '"' || element[pos] == '\'') {
        // Quoted, look for matching quote
        endPos = lElement.find(element[pos], pos + 1);
        ++pos; // Skip start quote
    }
    else {
        // Unquoted, look for first whitespace
        endPos = element.find_first_of("> \t\n\r", pos);
    }

    // Return the result, if any
    if (endPos == string::npos) {
        return ""; // Couldn't find the end
    } else {
        return element.substr(pos, endPos - pos);
    }
}

// --------------------------------------------------------------------------
size_t SXML::find(const string& element, SXML& output, size_t search) const
{
    // Try to find an element
    size_t start = findElement(xml, "<" + element, search);
    if (start==string::npos) {
        // Didn't find anything
        return string::npos;
    }

    // Found the start, let's look for the end
    // **NOTE: This is a very simple library and doesn't support nested elements.
    size_t end = xml.find("</"+element+">", start + 1);
    if (end==string::npos) {
        // Couldn't find the end, malformed XML
        return string::npos;
    }

    // Calculate the length of this element and contents
    size_t length = end-start;
    length += element.size()+3; // </p>
    output.xml = xml.substr(start, length);
    return start+length;
}

// --------------------------------------------------------------------------
size_t SXML::findElement(const string& lHaystack, const string& opening, size_t searched)
{
    // Search for the next thing starting with "<blah"
    while (true) {
        // Verify it's not "<blahblah>" by checking to see it's followed by
        // " " or ">"
        const size_t openPos = lHaystack.find(opening, searched);
        if (openPos == string::npos) {
            return string::npos;
        }
        const char nextChar = lHaystack[openPos + opening.size()];

        // if the next character after the tag type is whitespace or a closing >, then it is valid.
        if (SContains("\t\r\n> ", nextChar)) {
            return openPos;
        }
        searched = openPos + opening.size();
    }
}

// --------------------------------------------------------------------------
list<string> SXML::getElementsByType(const string& haystack, const string& type)
{
    list<string> elementList;
    getElementsByType(haystack, type, elementList);
    return elementList;
}

// --------------------------------------------------------------------------
bool SXML::getElementsByType(const string& haystack, const string& type, list<string>& elementList)
{
    // Convert the haystack to lowercase so we do case insensitive searching
    const string& lHaystack = SToLower(haystack);

    // Find the opening of that type
    const string& lType   = SToLower(type);
    const string& opening = "<" + lType;
    const string& closing = "</" + lType + ">";
    size_t searched = 0;
    while (true) {
        // Find the start of the element
        size_t openPos = SXML::findElement(lHaystack, opening, searched);
        if (openPos == string::npos) {
            break;
        }

        // Found the start of a element, now look for the end
        // **FIXME: Only finds strict matches of the end, like </blah>, but not </ blah>
        size_t closePos    = lHaystack.find(closing, openPos + opening.size());
        size_t nextOpenPos = SXML::findElement(lHaystack, opening, openPos + opening.size());
        size_t length;
        if (closePos == string::npos || nextOpenPos < closePos) {
            // Can't find the closing tag for this element, or the same type
            // of element is opened again before the next closing tag found.  So,
            // let's assume this is a "closed form" element tag, and just return
            // everything in the header
            size_t closeBracePos = lHaystack.find(">", openPos + opening.size());
            SASSERTWARN(closeBracePos != string::npos);
            if (closeBracePos == string::npos) {
                break;
            }
            length = (closeBracePos - openPos) + 1;
        }
        else {
            // Found a closing tag, use that
            length = (closePos - openPos) + closing.size();
        }

        // Add that element and search for the next
        const string& element = haystack.substr(openPos, length); // Use proper case for result
        elementList.push_back(element);
        searched = openPos + length;
    }

    // Return whether there are any results
    return !elementList.empty();
}

// --------------------------------------------------------------------------
string SXML::getType(const string& node)
{
    // Make sure this is a node
    if (node.empty() || node[0] != '<') {
        return ""; // Malformed
    }

    // Just whatever's after the open
    size_t after = node.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", 1);
    if (after == string::npos) {
        return ""; // Something funky, ignore
    }

    // Got it
    return SToLower(node.substr(1, after - 1));
}

// --------------------------------------------------------------------------
string SXML::decode(const string& text)
{
    // Replace any XML entities
    string    utext        = SToUpper(text);
    size_t    c            = 0;
    string    out;
    const int NUM_ENTITIES = 6;
    const char* entity[]  = {"&AMP;", "&QUOT;", "&APOS;", "&LT;", "&GT;", "&NBSP;", 0};
    const char* replace[] = {"&", "\"", "'", "<", ">", " ", 0};
    while (c < text.size()) {
        // Do we have an escape?
        char ch  = text[c];
        int  ent = 0;
        if (ch == '&') {
            for (ent = 0; ent < NUM_ENTITIES; ++ent) {
                if (utext.find(entity[ent], c) == c) {
                    // Found it
                    c += strlen(entity[ent]);
                    out += replace[ent];
                    break;
                }
            }
        }

        // Did we do it?
        if (ch != '&' || ent == NUM_ENTITIES) {
            // Nope, just add this character
            c += 1;
            out += ch;
        }
    }
    return out;
}

// --------------------------------------------------------------------------
string SXML::getText(const string& element)
{
    // Make sure this is a element
    if (element.empty() || element[0] != '<' || element[element.size() - 1] != '>') {
        return "";
    } // Malformed

    // Find the start and end of the text
    size_t textStart = element.find(">");
    size_t textEnd   = element.find_last_of("<");
    if (textStart >= textEnd) {
        return "";
    } // No text

    // Found some text, decode any XML entities inside
    return SStripTrim(SXML::decode(element.substr(textStart + 1, textEnd - textStart - 1)));
}

// --------------------------------------------------------------------------
string SXML::getElementByTypeAndAttribute(const string& haystack, const string& type, const string& name, const string& value)
{
    // First get a list of everything with that type, then return the first with the proper name/value
    list<string> elementList = SXML::getElementsByType(haystack, type);
    for (string& element : elementList) {
        if (SStripTrim(SXML::getAttribute(element, name)) == value) {
            return element;
        }
    }

    // Didn't find it
    return "";
}

// --------------------------------------------------------------------------
string SXML::entities(const string& value)
{
    string out = value;
    out = SReplace(out, "&", "&amp;");
    out = SReplace(out, "'", "&apos;");
    out = SReplace(out, "\"", "&quot;");
    out = SReplace(out, "<", "&lt;");
    out = SReplace(out, ">", "&gt;");
    return out;
}

// --------------------------------------------------------------------------
void SXML::test()
{
    // Simple test of this XML library
    SASSERTEQUALS(SXML::getText("<blah>foo bar</blah>"), "foo bar" );
    SASSERTEQUALS(SXML::getText("<blah> foo bar </blah>"), "foo bar" );
    SASSERTEQUALS(SXML::getText("<blah>foo&amp;bar</blah>"), "foo&bar" );
    SASSERTEQUALS(SXML::getText("<blah>foo&QUOT;bar</blah>"), "foo\"bar" );
    SASSERTEQUALS(SXML::getText("<blah>foo&APoS;bar</blah>"), "foo'bar" );
    SASSERTEQUALS(SXML::getText("<blah>foo&Lt;bar</blah>"), "foo<bar" );
    SASSERTEQUALS(SXML::getText("<blah>foo&gT;bar</blah>"), "foo>bar" );
    SASSERTEQUALS(SXML::getText("<blah>foo&none;bar</blah>"), "foo&none;bar" );
    SASSERTEQUALS(SXML::entities("abc<>&'\""), "abc&lt;&gt;&amp;&apos;&quot;" );

    // Test finding two elements in an XML doc
    SXML xml("<p>This is a <b>paragraph</b> with <b>bold</b></p>");
    SXML out;
    size_t skip=xml.find("b", out);
    SASSERT(skip!=string::npos);
    SASSERTEQUALS(out.text(), "paragraph");
    skip=xml.find("b", out, skip);
    SASSERTEQUALS(out.text(), "bold");
}
