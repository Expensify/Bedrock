#pragma once

// Simple class for processing XML
// Ex: SXML("<blah>asfd</blah>")["blah"].text()
class SXML
{
public:
    /**
     * Construct a new empty SXML document
     */
    SXML() : xml()
    { }

    /**
     * Construct a new SXML document with a well formed XML string.
     *
     * @param xml like: "<blah>asfd</blah>"
     */
    SXML(const string& xml) : xml(xml)
    { }

    /**
     * Searches for the first element of a type in the XML input
     *
     * @param element What type of element we're searching for (eg, "p");
     * @param output  Reference to fill with the output element
     * @param search  Location in the input where to start searching (default 0)
     * @returns       Location after the end of this element, or string::npos if not found
     */
    size_t find(const string& element, SXML& output, size_t search=0) const;

    /**
     * Looks up a descendent-element
     *
     * @param rhs a node type like "blah"
     * @return a new SXML object.
     */
    SXML operator[](const string& rhs) const
    {
        return SXML(getElementByType(xml, rhs));
    }

    /**
     * Get the text from this element
     *
     * @return The text, like "asdf"
     */
    string text() const
    {
        return getText(xml);
    }

    /**
     * Looks up an attribute of this element
     *
     * @param the name of the attribute
     * @return The attribute value
     */
    string attribute(const string& name) const
    {
        return getAttribute(xml, name);
    }

    /**
     * Search for an element with a given attribute set to a given value
     *
     * @param element
     * @param name
     * @param value
     * @return a new SXML object
     */
    SXML search(const string& element, const string& name, const string& value) const
    {
        return SXML(getElementByTypeAndAttribute(xml, element, name, value));
    }

    //////////
    // STATIC GARBAGE
    //////////

    /**
     * Find the element attribute with the given attribute
     *
     * @param element
     * @param name
     * @return attribute the value
     */
    static string getAttribute(const string& element, const string& name);

    /**
     * Finds the first element of a particular type
     *
     * @param haystack
     * @param type
     * @return element
     */
    static string getElementByType(const string& haystack, const string& type);

    /**
     * Finds all elements of a particular type
     *
     * @param haystack
     * @param type
     * @return element list
     */
    static list<string> getElementsByType(const string& haystack, const string& type);

    /**
     * Finds all elements of a particular type
     *
     * @param haystack
     * @param type
     * @param elementList Populated with found elements
     * @return were any found?
     */
    static bool getElementsByType(const string& haystack, const string& type, list<string>& elementList);

    /**
     * Find an element of a particular type, with a given attribute set to a given value
     *
     * @param haystack XML to search
     * @param type
     * @param name
     * @param value
     * @return an XML string
     */
    static string getElementByTypeAndAttribute(const string& haystack, const string& type, const string& name, const string& value);

    /**
     * Get the element type of the given node, or "" if invalid
     *
     * @param node
     * @return The type, like "blah"
     */
    static string getType(const string& node);

    /**
     * Get the text from this element
     *
     * @param node
     * @return The text, like "asdf"
     */
    static string getText(const string& node);

    /**
     * Safely encodes any xml special entities.
     *
     * @param text The text with potentially unsafe entities
     * @return The text with any entities encoded.
     */
    static string entities(const string& value);

    /**
     * Converts any xml encoded entities back to their decoded counterparts
     *
     * @param text The text with encoded entities
     * @return The text with any entities decoded.
     */
    static string decode(const string& text);

    /**
     * Executes a basic series of internal tests
     */
    static void test();

    /**
     * Finds the next instance of an element and returns its position.
     *
     * @param lHaystack Original XML to search (eg, `<foo><bar a="1">asdf</bar></foo>`)
     * @param opening   The opening brace to look for (eg, "<blah")
     * @param searched  However much to skip in front (ie, has already been searched)
     * @return          Location of the element, or string::npos if not found
     */
    static size_t findElement(const string& lHaystack, const string& opening, size_t searched);

    // Internal copy of the element to be manipulated
    string xml;
};

