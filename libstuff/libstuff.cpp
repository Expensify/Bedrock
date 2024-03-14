// --------------------------------------------------------------------------
// libstuff.cpp
// --------------------------------------------------------------------------
// C library
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <execinfo.h>
#include <sys/un.h>
#include <cxxabi.h>
#include <sys/ioctl.h>

#include "libstuff.h"
#include <sys/stat.h>
#include <zlib.h>

#include <mbedtls/aes.h>
#include <mbedtls/base64.h>
#include <mbedtls/sha1.h>
#include <mbedtls/sha256.h>

#include <libstuff/SQResult.h>
#include <libstuff/SData.h>
#include <libstuff/SFastBuffer.h>
#include <libstuff/sqlite3.h>

// Additional headers
#include <netdb.h>
#include <sys/time.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>
#ifdef __APPLE__
// Apple specific tweaks
#include <sys/types.h>
#include <sys/stat.h>
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
#endif

#include <pcrecpp.h> // sudo apt-get install libpcre++-dev

// Common error definitions
#define S_errno errno
#define S_NOTINITIALISED ((int)0xFEFEFEFE) // Doesn't exist for Linux
#ifdef __APPLE__
// The above doesn't even build on OS X with C++11 turned on. I don't know why
// we even have that check, but I'm leaving it and just setting this to INT_MAX
// on OS X. -Tyler
#undef S_NOTINITIALISED
#define S_NOTINITIALISED INT_MAX
#endif
#define S_EAGAIN EAGAIN
#define S_ENETDOWN ENETDOWN
#define S_EFAULT EFAULT
#define S_ENETRESET ENETRESET
#define S_EISCONN EISCONN
#define S_ENOTSOCK ENOTSOCK
#define S_EOPNOTSUPP EOPNOTSUPP
#define S_ESHUTDOWN ESHUTDOWN
#define S_EINVAL EINVAL
#define S_ETIMEDOUT ETIMEDOUT
#define S_ECONNRESET ECONNRESET
#define S_EINTR EINTR
#define S_EINPROGRESS EINPROGRESS
#define S_EWOULDBLOCK EWOULDBLOCK
#define S_EMSGSIZE EMSGSIZE
#define S_EMFILE EMFILE
#define S_ENOBUFS ENOBUFS
#define S_ENOTCONN ENOTCONN
#define S_ECONNABORTED ECONNABORTED
#define S_ECONNREFUSED ECONNREFUSED
#define S_EACCES EACCES
#define S_EHOSTUNREACH EHOSTUNREACH
#define S_EALREADY EALREADY
#define S_EPIPE EPIPE

#define S_COOKIE_SEPARATOR ((char)0xFF)

thread_local string SThreadLogPrefix;
thread_local string SThreadLogName;
thread_local bool isSyncThread;

// We store the process name passed in `SInitialize` to use in logging.
thread_local string SProcessName;

// This is a set of reusable sockets, each with an associated mutex, to allow for parallel logging directly to the
// syslog socket, rather than going through the syslog() system call, which goes through journald.
const size_t S_LOG_SOCKET_MAX = 500;
int SLogSocketFD[S_LOG_SOCKET_MAX] = {};
mutex SLogSocketMutex[S_LOG_SOCKET_MAX];
atomic<size_t> SLogSocketCurrentOffset(0);
struct sockaddr_un SLogSocketAddr;
atomic_flag SLogSocketsInitialized = ATOMIC_FLAG_INIT;

// Set to `syslog` or `SSyslogSocketDirect`.
atomic<void (*)(int priority, const char *format, ...)> SSyslogFunc = &syslog;

void SInitialize(string threadName, const char* processName) {
    isSyncThread = false;
    // This is not really thread safe. It's guaranteed to run only once, because of the atomic flag, but it's not
    // guaranteed that a second caller to `SInitialize` will wait until this block has completed before attempting to
    // use the socket logging variables. This is handled by the fact that we call `SInitialize` in main() which waits
    // for the completion of the call before any other threads can be initialized.
    if (!SLogSocketsInitialized.test_and_set()) {
        SLogSocketAddr.sun_family = AF_UNIX;
        strcpy(SLogSocketAddr.sun_path, "/run/systemd/journal/syslog");
        for (size_t i = 0; i < S_LOG_SOCKET_MAX; i++) {
            SLogSocketFD[i] = -1;
        }
    }

    // We statically store whichever process name was passed most recently to reuse. This lets new threads start up
    // with the same process name as existing threads, even when using socket logging, since "openlog" has no effect
    // then.
    static string initialProcessName = processName ? processName : "bedrock";
    SProcessName = initialProcessName;

    // If a specific process name has been supplied, initialize syslog with it.
    if (processName) {
        openlog(processName, 0, 0);
    }

    // Initialize signal handling
    SLogSetThreadName(threadName);
    SLogSetThreadPrefix("xxxxxx ");
    SInitializeSignals();
}

// Thread-local log prefix
void SLogSetThreadPrefix(const string& logPrefix) {
    SThreadLogPrefix = logPrefix;
}

void SLogSetThreadName(const string& logName) {
    SThreadLogName = logName;
}

SException::SException(const string& file,
                       int line,
                       bool generateCallstack,
                       const string& _method,
                       const STable& _headers,
                       const string& _body)
  : _file(file), _line(line), method(_method), headers(_headers), body(_body) {
    // Build a callstack. We don't convert it to symbols unless someone asks for it later.
    if (generateCallstack) {
        _depth = backtrace(_callstack, CALLSTACK_LIMIT);
    }
    SINFO("Throwing exception with message: '" << _method << "' from " << file << ":" << line);
}

const char* SException::what() const noexcept {
    return method.c_str();
}

vector<string> SGetCallstack(int depth, void* const* callstack) noexcept {
    // Symbols for each stack frame.
    char** symbols = nullptr;
    symbols = backtrace_symbols(callstack, depth);

    vector<string> details(depth + 1);
    int status = 0;
    for (int i = 0; i < depth; i++) {
        // Demangle them if possible.
        string temp = symbols[i];
        size_t start = temp.find_first_of('(');
        size_t end = temp.find_first_of('+', start);
        temp = temp.substr(start + 1, end - start - 1);
        char* demangled = abi::__cxa_demangle(temp.c_str(), 0, 0, &status);

        // If the status is OK, we'll see if we can pull the address from the original string.
        // If so, we concatenate that on the end of the demangled line. If we can't pull it out, we'll fall back to the
        // original line, as we'd rather have mangled names with potential offsets than demangled names but lost
        // offsets.
        if (status == 0) {
            string symbolsStr = string(symbols[i]);
            size_t addressOffset = symbolsStr.find_last_of('[');
            if (addressOffset != string::npos) {
                details[i + 1] = string(demangled) + " " + symbolsStr.substr(addressOffset);
            } else {
                details[i + 1] = symbols[i];
            }
        } else {
            details[i + 1] = symbols[i];
        }
        free(demangled);
    }
    return details;
}

vector<string> SException::details() const noexcept {
    vector<string> stack = SGetCallstack(_depth, _callstack);
    stack.push_back(string("Initially thrown from: ") + basename((char*)_file.c_str()) + ":" + to_string(_line));
    return stack;
}

void SSyslogSocketDirect(int priority, const char *format, ...) {
    int socketError = 0;
    static const size_t MAX_MESSAGE_SIZE = 8 * 1024;

    // Choose an FD from our array.
    size_t socketIndex = (++SLogSocketCurrentOffset) % S_LOG_SOCKET_MAX;

    // Lock for this particular socket. If more than one thread want to log using the same FD, some will wait.
    lock_guard<mutex> lock(SLogSocketMutex[socketIndex]);

    if (SLogSocketFD[socketIndex] == -1) {
        // Create a socket if there isn't one already.
        SLogSocketFD[socketIndex] = socket(AF_UNIX, SOCK_DGRAM, 0);
    }
    if (SLogSocketFD[socketIndex] == -1) {
        // Error opening the socket. We'll fall back to regular syslog.
        socketError = errno;
    } else {
        // At this point, the socket should be good.

        // This is based on the message format here: https://tools.ietf.org/html/rfc5424#section-6 but doesn't actually
        // match the format there, as it seems syslog ignores almost the entire "header" and "structured data" fields
        // and treats them as part of the message.
        string messageHeader = "<" + to_string(8 + priority) + ">" + SProcessName + ": ";
        thread_local char messageBuffer[MAX_MESSAGE_SIZE];
        strcpy(messageBuffer, messageHeader.c_str());
        va_list argptr;
        va_start(argptr, format);
        int bytesWritten = vsnprintf(messageBuffer + messageHeader.size(), MAX_MESSAGE_SIZE - messageHeader.size(), format, argptr);
        va_end(argptr);


        // Assume we send the whole message. We don't do anything to handle message truncation.
        ssize_t bytesSent = sendto(SLogSocketFD[socketIndex], messageBuffer, bytesWritten + messageHeader.size(), 0, (const struct sockaddr *)&SLogSocketAddr, sizeof(struct sockaddr_un));
        if (bytesSent == -1) {
            socketError = errno;
            close(SLogSocketFD[socketIndex]);
            SLogSocketFD[socketIndex] = -1;
        }
    }

    if (socketError) {
        syslog(LOG_WARNING, "Could not use direct logging socket (error: %i, %s), falling back to syslog syscall.", socketError, strerror(socketError));
        va_list argptr;
        va_start(argptr, format);
        vsyslog(priority, format, argptr);
        va_end(argptr);
    }
}

/////////////////////////////////////////////////////////////////////////////
// Math stuff
/////////////////////////////////////////////////////////////////////////////

// --------------------------------------------------------------------------
string SToHex(uint64_t value, int digits) {
    // Allocate the string and fill from back to front
    string working;
    working.resize(digits);
    for (int c = digits - 1; c >= 0; --c) {
        // Get the hex digit and add
        char digit = (char)(value % 16);
        value /= 16;
        working[c] = (digit < 10 ? '0' + digit : 'A' + (digit - 10));
    }
    return working;
}

string SToHex(const string& value) {
    // Fill from front to back
    string working;
    working.reserve(value.size() * 2);
    for (size_t c = 0; c < value.size(); ++c) {
        // Add two digits per byte
        unsigned char digit = (unsigned char)value[c];
        working += ((digit >> 4) < 10 ? '0' + (digit >> 4) : 'A' + (digit >> 4) - 10);
        working += ((digit & 0xF) < 10 ? '0' + (digit & 0xF) : 'A' + (digit & 0xF) - 10);
    }
    return working;
}

string SToHex(uint32_t value) {
    return SToHex(value, 8);
}

// --------------------------------------------------------------------------
uint64_t SFromHex(const string& value) {
    // Convert one digit at a time
    uint64_t binValue = 0;
    for (int c = 0; c < (int)value.size(); ++c) {
        // Shift over the previous and add this byte
        binValue <<= 4;
        if ('0' <= value[c] && value[c] <= '9')
            binValue += value[c] - '0';
        else if ('a' <= value[c] && value[c] <= 'f')
            binValue += value[c] - 'a' + 10;
        else if ('A' <= value[c] && value[c] <= 'F')
            binValue += value[c] - 'A' + 10;
        else {
            // Invalid character, undo shift and done parsing
            binValue >>= 4;
            break;
        }
    }
    return binValue;
}

string SStrFromHex(const string& buffer) {
    string retVal;
    retVal.reserve(buffer.size() / 2);
    for(size_t i = 0; i < buffer.size(); i += 2) {
        retVal.push_back((char)strtol(buffer.substr(i, 2).c_str(), 0, 16));
    }
    return retVal;
}

string SBase32HexStringFromBase32(const string& buffer) {
    static const char map[] = "QRSTUV\0\0\0\0\0\0\0\0\0""0123456789ABCDEFGHIJKLMNOP";
    static const int mapLength = sizeof(map);
    string out = buffer;
    int shiftedIndex;
    for (size_t i = 0; i < out.size(); i++) {
        shiftedIndex = out[i] - 50;
        if (mapLength < shiftedIndex || map[shiftedIndex] == 0) {
            STHROW("Character not found in base32 alphabet.");
        }
        out[i] = map[shiftedIndex];
    }

    return out;
}

string SHexStringFromBase32(const string& buffer) {
    if (buffer.length() % 8 != 0) {
        STHROW("Incorrect string length.");
    }

    string hex;
    for (size_t i = 0; i < buffer.length(); i += 8) {
        uint64_t val = stoull(buffer.substr(buffer.length() - 8 - i, 8), 0, 32);
        hex.insert(0, SToHex(val, 10));
    }

    return hex;
}

/////////////////////////////////////////////////////////////////////////////
// String stuff
/////////////////////////////////////////////////////////////////////////////
string SToLower(string value) {
    transform(value.begin(), value.end(), value.begin(), ::tolower);
    return value;
}

string SToUpper(string value) {
    transform(value.begin(), value.end(), value.begin(), ::toupper);
    return value;
}

bool SIContains(const string& lhs, const string& rhs) {
    // Case insensitive contains
    return SContains(SToLower(lhs), SToLower(rhs));
}

bool SStartsWith(const string& haystack, const string& needle) {
    return SStartsWith(haystack.c_str(), haystack.size(), needle.c_str(), needle.size());
}

bool SStartsWith(const char* haystack, size_t haystackSize, const char* needle, size_t needleSize) {
    if (haystackSize < needleSize) {
        return false;
    }
    return strncmp(haystack, needle, needleSize) == 0;
}

// --------------------------------------------------------------------------
string STrim(const string& lhs) {
    // Just trim off the front and back whitespace
    if(!lhs.empty()) {
        const char* front(lhs.data());
        const char* back(&lhs.back());
        while(*front && isspace(*front))
            ++front;
        while(back > front && isspace(*back))
            --back;
        return string(front, ++back);
    }
    return "";
}

// --------------------------------------------------------------------------
string SCollapse(const string& lhs) {
    // Collapse all whitespace into a single space
    string out;
    out.reserve(lhs.size());
    bool inWhite = false;
    for (const char* c(lhs.data()); *c; ++c)
        if (isspace(*c)) {
            // Only add if not already whitespace
            if (!inWhite)
                out += ' ';
            inWhite = true;
        } else {
            // Not whitespace, add
            out += *c;
            inWhite = false;
        }
    return out;
}

// --------------------------------------------------------------------------
string SStrip(const string& lhs) {
    // Strip out all non-printable characters
    string working;
    working.reserve(lhs.size());
    for (const char* c(lhs.data()); *c; ++c)
        if (isprint(*c))
            working += *c;
    return working;
}

// --------------------------------------------------------------------------
string SStrip(const string& lhs, const string& chars, bool charsAreSafe) {
    // Strip out all unsafe characters
    string working;
    working.reserve(lhs.size());
    for (const char* c(lhs.data()); *c; ++c) {
        // If the characters are in the set and are safe, then add.
        // Otherwise, if the characters are unsafe but not in the set, still add.
        bool inSet = (chars.find(*c) != string::npos);
        if (inSet == charsAreSafe)
            working += *c;
    }
    return working;
}

// --------------------------------------------------------------------------
string SEscape(const char* lhs, const string& unsafe, char escaper) {
    // Escape all unsafe characters
    string working;
    while (*lhs) {
        // Insert an escape if an unsafe characater
        if (unsafe.find(*lhs) != string::npos || *lhs == escaper) {
            // Insert the escape
            const char& c = *lhs;
            working += escaper;
            if (c == '\b')
                working += 'b';
            else if (c == '\f')
                working += 'f';
            else if (c == '\n')
                working += 'n';
            else if (c == '\r')
                working += 'r';
            else if (c == '\t')
                working += 't';
            else if ((c > 0x00 && c < 0x20) || c == 0x7f) {
                char utfCode[6] = {0};
                sprintf(utfCode, "u%04x", c);
                working += utfCode;
            } else {
                working += c;
            }
        } else {
            // Insert as normal
            working += *lhs;
        }
        ++lhs;
    }
    return working;
}

// --------------------------------------------------------------------------
string SUnescape(const char* lhs, char escaper) {
    // Fix all escaped values
    string working;
    for (; *lhs; ++lhs) {
        // Insert an escape if an unsafe characater
        if (*lhs == escaper && *(lhs + 1)) // Make sure there's another
        {
            // See if the next character is special
            ++lhs; // Skip escaper
            const char& c = *lhs;
            if (c == 'b')
                working += '\b';
            else if (c == 'f')
                working += '\f';
            else if (c == 'n')
                working += '\n';
            else if (c == 'r')
                working += '\r';
            else if (c == 't')
                working += '\t';
            else if (c == 'u' && strlen(lhs + 1) >= 4) {
                // Scan the UTF-8 value into an int.
                // NOTE: JSON only supports 4 hex digits in escaped unicode sequences.
                char utfCode[5] = {0};
                unsigned int utfValue = 0;
                int additionalBytes = 0;
                strncpy(utfCode, lhs + 1, 4);
                sscanf(utfCode, "%04x", &utfValue);
                lhs += 4;

                // 7 or fewer bits goes straight through
                if (utfValue <= 0x007f) {
                    working += (char)utfValue;
                    utfValue = 0;
                    additionalBytes = 0;
                }

                // 8 to 11 bits turns into 2 Byte sequence:
                // 110x.xxxx 10xx.xxxx
                else if (utfValue <= 0x07ff) {
                    // UTF-8 2 byte header is 110.
                    // 1100.0000 | Top 5 bits of utfValue
                    char byte = 0xc0 | (utfValue >> 6);
                    working += byte;

                    // Cancel out the bits we just used.
                    utfValue &= ~(byte << 6);
                    additionalBytes = 1;
                }

                // 9 to 16 bits turns into 3 Byte sequence:
                // 1110.xxxx 10xx.xxxx 10xx.xxxx
                else if (utfValue <= 0xffff) {
                    // UTF-8 3 byte header is 1110.
                    // 1110.0000 | Top 4 bits of utfValue.
                    char byte = 0xe0 | (utfValue >> 12);
                    working += byte;

                    // Cancel out the bits we just used.
                    utfValue &= ~(byte << 12);
                    additionalBytes = 2;
                }

                // Loop through the remaining bytes expanding with UTF-8 header
                string utfChar = "";
                for (; additionalBytes > 0; --additionalBytes) {
                    // UTF-8 trailing byte header is 10.
                    // 1000.0000 | The bottom 6 bits of utfValue.
                    char byte = 0x80 | (utfValue & 0x003f);

                    // Shift out the bottom 6 bits (we just used them above).
                    utfValue >>= 6;

                    // Prepend this expanded byte to the utfChar.
                    utfChar = byte + utfChar;
                }
                working += utfChar;
            } else
                working += c;
        }

        // Insert as normal
        else
            working += *lhs;
    }
    return working;
}

// --------------------------------------------------------------------------
string SReplace(const string& value, const string& find, const string& replace) {
    // What are you trying to pull sending an empty string here?
    if (find.empty())
        return value;

    // Keep going until we find no more
    string out;
    out.reserve(value.size());
    size_t skip = 0;
    while (true) {
        // Look for the next match
        size_t pos = value.find(find, skip);
        if (pos == string::npos) {
            // Add the rest and done
            out += value.substr(skip);
            return out;
        }

        // Replace
        out += value.substr(skip, pos - skip);
        out += replace;
        skip = pos + find.size();
    }
}

// --------------------------------------------------------------------------
string SReplaceAllBut(const string& value, const string& safeChars, char replaceChar) {
    // Loop across the string and replace any invalid character
    string out;
    out.reserve(value.size());
    for (const char* c(value.data()); *c; ++c)
        if (safeChars.find(*c) != string::npos)
            out += *c;
        else
            out += replaceChar;
    return out;
}

// --------------------------------------------------------------------------
string SReplaceAll(const string& value, const string& unsafeChars, char replaceChar) {
    // Loop across the string and replace any invalid character
    string out;
    out.reserve(value.size());
    for (const char* c(value.data()); *c; ++c)
        if (unsafeChars.find(*c) == string::npos)
            out += *c;
        else
            out += replaceChar;
    return out;
}

// --------------------------------------------------------------------------
int SStateNameToInt(const char* states[], const string& stateName, unsigned int numStates) {
    // Converts an array of state names back to the index
    for (int i = 0; i < (int)numStates; i++)
        if (SIEquals(states[i], stateName))
            return i;
    return -1;
}

// --------------------------------------------------------------------------
bool SConstantTimeEquals(const string& secret, const string& userInput) {
    // If one (and only one) of the parameters is zero length, fail now.  This
    // leaks no timing information and keeps us from having to worry about
    // dividing by zero below.
    if (secret.empty() != userInput.empty()) {
        return false;
    }

    // If the lengths are unequal, the strings are not equal. However, we still
    // have to do the comparison.
    bool equal = secret.length() == userInput.length();
    for (size_t i = 0; i < secret.length(); i++) {
        // Do a constant number of comparisons, equal to the length of the secret.
        // To keep the number constant, wrap around userInput if it is too short.
        equal &= secret[i] == userInput[i % userInput.length()];
    }
    return equal;
}

// --------------------------------------------------------------------------
bool SConstantTimeIEquals(const string& secret, const string& userInput) {
    // Case insensitive comparison
    return SConstantTimeEquals(SToLower(secret), SToLower(userInput));
}

// --------------------------------------------------------------------------
list<int64_t> SParseIntegerList(const string& value, char separator) {
    list<int64_t> valueList;
    list<string> strings = SParseList(value, separator);
    for (string str : strings) {
        valueList.push_back(SToInt64(str));
    }
    return valueList;
}

// --------------------------------------------------------------------------
set<int64_t> SParseIntegerSet(const string& value, char separator) {
    set<int64_t> valueSet;
    list<string> strings = SParseList(value, separator);
    for (const string& str : strings) {
        valueSet.insert(SToInt64(str));
    }
    return valueSet;
}

// --------------------------------------------------------------------------
bool SParseList(const char* ptr, list<string>& valueList, char separator) {
    // Clear the input
    valueList.clear();

    // Walk across the string and break into comma/whitespace delimited substrings
    string component;
    while (*ptr) {
        // Is this the start of a new string?  If so, ignore to trim leading whitespace.
        if (component.empty() && *ptr == ' ') {
        }

        // Is this a delimiter?  If so, let's add our current component to the list and start a new one
        else if (*ptr == separator) {
            // Only add if the component is non-empty
            if (!component.empty())
                valueList.push_back(component);
            component.clear();
        }

        // Otherwise, add to the working component
        else {
            component += *ptr;
        }

        // Finally, go to the next character
        ++ptr;
    }

    // Reached the end of the string; if we are working on a component, add it
    if (!component.empty())
        valueList.push_back(component);

    // Return if we were able to find anything
    return (!component.empty());
}

STable SParseCommandLine(int argc, char* argv[]) {
    // Just walk across and find the pairs, then put the remainder on a list in the method
    STable results;
    string name;
    for (int c = 1; c < argc; ++c) {
        // Does this look like a name or value?
        bool isName = SStartsWith(argv[c], "-");
        if (name.empty()) {
            // We're not already processing a name, either start or add
            if (isName) {
                name = argv[c];
            }
        } else {
            // Processing a name, do we have a value or another name?
            if (isName) {
                // Set the working name to blank and use the new name
                results[name] = "";
                name = argv[c];
            } else {
                // Associate this value and clear
                results[name] = argv[c];
                name.clear();
            }
        }
    }

    // If any leftover name, just set empty
    if (!name.empty()) {
        results[name] = "";
    }

    return results;
}

/////////////////////////////////////////////////////////////////////////////
// Network stuff
/////////////////////////////////////////////////////////////////////////////
// --------------------------------------------------------------------------
const char* _SParseHTTP_GetUpToNext(const char* start, const char* lineEnd, char separator, string& out) {
    // Trim leading whitespace
    while (*start == ' ')
        ++start;

    // Look for the separator
    const char* end = start;
    while ((end < lineEnd) && (*end != separator))
        ++end;
    const char* separatorPos = end;

    // Found the separator, trim off any trailing whitespace
    while (*(end - 1) == ' ')
        --end;

    // If there's anything left, that's the output
    int length = (int)(end - start);
    if (length > 0) {
        // Found, output
        out.resize(length);
        memcpy(&out[0], start, length);
    }

    // Return the new parse location
    return separatorPos;
}

// --------------------------------------------------------------------------
void _SParseHTTP_GetUpToEnd(const char* start, const char* end, string& out) {
    // Get everything up to the end of the line, triming leading and trailing whitespace
    while (*start == ' ')
        ++start;
    while (*(end - 1) == ' ')
        --end;
    int length = (int)(end - start);
    if (length > 0) {
        // Copy the output
        out.resize(length);
        memcpy(&out[0], start, length);
    }
}

// --------------------------------------------------------------------------
int SParseHTTP(const char* buffer, size_t length, string& methodLine, STable& nameValueMap, string& content) {
    // Clear the output
    methodLine.clear();
    nameValueMap.clear();
    content.clear();

    // Keep parsing until we run out of input or encounter a blank line
    const char* lineStart = buffer;
    const char* inputEnd = buffer + length;
    string name;
    bool isChunked = false;
    bool lastChunkFound = false;
    while (lineStart < inputEnd) {
        // Find the end of the line
        const char* lineEnd = lineStart;
        while ((lineEnd < inputEnd) && (*lineEnd != '\r') && (*lineEnd != '\n'))
            ++lineEnd;
        if (lineEnd >= inputEnd) {
            // Couldn't find end of line; couldn't complete parsing.
            methodLine.clear();
            nameValueMap.clear();
            content.clear();
            return 0;
        }

        // Found the end of the line; is the line blank?
        if (lineEnd == lineStart) {
            // Blank line -- if we have at least the method, then we're done.  Otherwise, ignore.

            // We want to look this up without inserting it.
            auto it = nameValueMap.find("Transfer-Encoding");
            if (!methodLine.empty()) {
                // If we are done processing a chunked body.
                if (isChunked) {
                    // Figure out the end of the message by consuming up to 2 EOL characters,
                    // then return the total length.
                    SASSERTWARN(lastChunkFound);
                    const char* parseEnd = lineEnd;
                    int numEOLs = 2;
                    while (parseEnd < inputEnd && (*parseEnd == '\r' || *parseEnd == '\n') && numEOLs--)
                        ++parseEnd;

                    // The SData object we've generated is not chunked, we remove this header as it does not describe the state of this request.
                    nameValueMap.erase("Transfer-Encoding");
                    return (int)(parseEnd - buffer);
                }

                // If not processing a chunked body, then finish up.
                else if (it == nameValueMap.end() || !SIEquals(it->second, "chunked")) {
                    // We have a method -- we're done.  Figure out the end of the message
                    // by consuming up to 2 EOL characters, then return the total length.
                    const char* parseEnd = lineEnd;
                    int numEOLs = 2;
                    while (parseEnd < inputEnd && (*parseEnd == '\r' || *parseEnd == '\n') && numEOLs--)
                        ++parseEnd;
                    int headerLength = (int)(parseEnd - buffer);

                    // If there is no content-length, just return the length of the headers
                    int contentLength = (SContains(nameValueMap, "Content-Length")
                                             ? atoi(nameValueMap["Content-Length"].c_str())
                                             : 0);
                    if (!contentLength)
                        return headerLength;

                    // There is a content length -- if we don't have enough, then cancel the parse.
                    if ((int)(length - headerLength) < contentLength) {
                        // Insufficient content
                        methodLine.clear();
                        nameValueMap.clear();
                        content.clear();
                        return 0;
                    }

                    // We have enough data -- copy it and return the full length
                    content.resize(contentLength);
                    memcpy(&content[0], parseEnd, contentLength);
                    return (headerLength + contentLength);
                }

                // Otherwise, we start on a chunked body.
                else
                    isChunked = true;
            }
        } else {
            // Not blank.  Is this the method line?
            bool isHeaderOrFooter = true;
            if (methodLine.empty()) {
                // Everything in the line is the method
                _SParseHTTP_GetUpToEnd(lineStart, lineEnd, methodLine);
                isHeaderOrFooter = false;
            }

            // Is it a new chunk?
            else if (isChunked) {
                // Get the chunk length and ignore the optional stuff after the optional semicolon.
                string chunkHeader;
                _SParseHTTP_GetUpToEnd(lineStart, lineEnd, chunkHeader);
                const string& hexChunkLength = SContains(chunkHeader, ";") ? SBefore(chunkHeader, ";") : chunkHeader;

                // If valid hex number, then we have a chunk.
                if (SREMatch("^[a-fA-F0-9]{1,8}$", hexChunkLength)) {
                    // Get the chunk length.
                    isHeaderOrFooter = false;
                    int chunkLength = (int)SFromHex(hexChunkLength);
                    if (chunkLength) {
                        // Verify that we can get the entire chunk.
                        const char* chunkStart = lineEnd + 2; // skipping the \r\n.
                        const char* chunkEnd = chunkStart + chunkLength;
                        if (chunkEnd >= inputEnd) {
                            // Insufficient content
                            methodLine.clear();
                            nameValueMap.clear();
                            content.clear();
                            return 0;
                        }

                        // Get the chunk and advance the pointers.
                        size_t contentLength = content.length();
                        content.resize(contentLength + chunkLength);
                        memcpy(&content[contentLength], chunkStart, chunkLength);
                        lineEnd = chunkEnd;
                    } else
                        lastChunkFound = true;
                }

                // Else it is a footer which should be treated just like a header.  Set it again for clarity.
                else
                    isHeaderOrFooter = true;
            }

            // More headers.
            if (isHeaderOrFooter) {
                // Does it start with whitespace?  If so, just append to the last value
                if (isspace(*lineStart)) {
                    // Starts with whitespace -- if we have a name, add it to the end of the last
                    // value.  Otherwise, add it to the end of the method.
                    if (!name.empty())
                        SAppend(nameValueMap[name], lineStart, (int)(lineEnd - lineStart));
                    else
                        SAppend(methodLine, lineStart, (int)(lineEnd - lineStart));
                } else {
                    // Parse name/value pair.  Name is everything up to the ':'
                    const char* nameEnd = _SParseHTTP_GetUpToNext(lineStart, lineEnd, ':', name);
                    if (!name.empty()) {
                        // The value is everything up to the end of the line,
                        // triming leading and trailing whitespace.
                        const char* valueStart = nameEnd + 1;
                        const char* valueEnd = lineEnd;
                        while (*valueStart == ' ')
                            ++valueStart;
                        while (*(valueEnd - 1) == ' ')
                            --valueEnd;
                        int valueLength = (int)(valueEnd - valueStart);
                        string value;
                        if (valueLength > 0) {
                            // Copy the value
                            value.resize(valueLength);
                            memcpy(&value[0], valueStart, valueLength);
                        }

                        // Store the result.  If there's something already
                        // there just override, with the exception of
                        // Set-Cookie: generate a crappy list with 0xFF
                        // separation.  (See SComposeHTTP for explanation.)
                        STable::iterator it = nameValueMap.find(name);
                        if (it == nameValueMap.end() || !SIEquals(name, "Set-Cookie"))
                            nameValueMap[name] = SUnescape(value); // strip any slash-escaping
                        else
                            nameValueMap[name] = it->second + S_COOKIE_SEPARATOR + value;
                    }
                }
            }
        }

        // Consume the end of the line -- accept \r\n, \n\r, \r, or \n.  But *not* \n\n (that's two endings)
        lineStart = lineEnd; // Advance past the parsed line to the line ending
        if (inputEnd - lineStart >= 2 && lineStart[0] == '\r' && lineStart[1] == '\n')
            lineStart += 2;
        else if (inputEnd - lineStart >= 2 && lineStart[0] == '\n' && lineStart[1] == '\r')
            lineStart += 2;
        else if (lineStart[0] == '\n')
            ++lineStart;
        else if (lineStart[0] == '\r')
            ++lineStart;
        else
            SWARN("How did we get here?");
    }

    // Reached the end of the input and haven't finished parsing the header
    methodLine.clear();
    nameValueMap.clear();
    content.clear();
    return 0;
}

// --------------------------------------------------------------------------
bool SParseRequestMethodLine(const string& methodLine, string& method, string& uri) {
    // Clear the input
    method.clear();
    uri.clear();

    // Parse the method line (everything up to the first space)
    const char* start = methodLine.c_str();
    const char* end = start + methodLine.size();
    const char* methodEnd = _SParseHTTP_GetUpToNext(start, end, ' ', method);
    _SParseHTTP_GetUpToNext(methodEnd + 1, end, ' ', uri);
    return (!method.empty() && !uri.empty());
}

// --------------------------------------------------------------------------
bool SParseResponseMethodLine(const string& methodLine, string& protocol, int& code, string& reason) {
    // Clear the input
    code = 0;
    reason.clear();

    // Parse the method line (everything up to the first space)
    string codeStr;
    const char* start = methodLine.c_str();
    const char* end = start + methodLine.size();
    const char* protocolEnd = _SParseHTTP_GetUpToNext(start, end, ' ', protocol);
    const char* codeEnd = _SParseHTTP_GetUpToNext(protocolEnd + 1, end, ' ', codeStr);
    _SParseHTTP_GetUpToEnd(codeEnd + 1, end, reason);
    code = atoi(codeStr.c_str());
    return (code && !reason.empty());
}

// --------------------------------------------------------------------------
int _SDecodeURIChar(const char* buffer, int length, string& out) {
    // No decoding if the buffer is too small or not encoded
    if (*buffer != '%' || length < 3) {
        // No decoding, just consume one character
        if (*buffer == '+')
            out += ' ';
        else
            out += *buffer;
        return 1;
    }

    // Decode three characters
    char outChar = 0;
    if (SWITHIN('0', buffer[1], '9'))
        outChar |= (buffer[1] - '0' + 0) << 4;
    else if (SWITHIN('a', buffer[1], 'f'))
        outChar |= (buffer[1] - 'a' + 10) << 4;
    else if (SWITHIN('A', buffer[1], 'F'))
        outChar |= (buffer[1] - 'A' + 10) << 4;
    else {
        // Invalid -- not sure what's going on.  Cancel decode.
        out += "%";
        out += buffer[1];
        return 2;
    }
    if (SWITHIN('0', buffer[2], '9'))
        outChar |= (buffer[2] - '0' + 0);
    else if (SWITHIN('a', buffer[2], 'f'))
        outChar |= (buffer[2] - 'a' + 10);
    else if (SWITHIN('A', buffer[2], 'F'))
        outChar |= (buffer[2] - 'A' + 10);
    else {
        // Invalid -- not sure what's going on.  Cancel decode.
        out += "%";
        out += buffer[1];
        out += buffer[2];
        return 3;
    }

    // Successful decode
    out += outChar;
    return 3;
}

bool SParseURI(const string& uri, string& host, string& path) {
    return SParseURI(uri.c_str(), (int)uri.size(), host, path);
}

bool SParseURI(const char* buffer, int length, string& host, string& path) {
    // Clear the output
    host.clear();
    path.clear();

    // Skip the protocol
    if (strncmp(buffer, "http://", strlen("http://")) && strncmp(buffer, "https://", strlen("https://")))
        return false; // Invalid URL
    int header = (int)(strstr(buffer, "//") - buffer) + 2;
    buffer += header;
    length -= header;

    // Get the host
    while (length > 0 && *buffer != '/') {
        // Add to the domain
        host += *buffer;
        ++buffer;
        --length;
    }

    // Is there anything left?
    if (!length) {
        // Empty path
        path = "/";
    } else {
        // Everything else is the path
        path.resize(length);
        memcpy(&path[0], buffer, length);
    }

    // Done
    return true; // Success
}

bool SParseURIPath(const string& uri, string& path, STable& nameValueMap) {
    return SParseURIPath(uri.c_str(), (int)uri.size(), path, nameValueMap);
}

bool SParseURIPath(const char* buffer, int length, string& path, STable& nameValueMap) {
    // Clear the output
    path.clear();
    nameValueMap.clear();

    // First, read everything in the path
    while (length > 0 && *buffer != '?') {
        // Consume some characters
        int consume = _SDecodeURIChar(buffer, length, path);
        length -= consume;
        buffer += consume;
    }

    // Skip over the '?'
    --length;
    ++buffer;

    // Get name/value pairs
    while (length > 0) {
        // Get the name
        string name;
        while (length > 0 && *buffer != '=') {
            // Consume some characters
            int consume = _SDecodeURIChar(buffer, length, name);
            length -= consume;
            buffer += consume;
        }

        // Skip over the '='
        --length;
        ++buffer;

        // Get the value
        string value;
        while (length > 0 && *buffer != '&') {
            // Consume some characters
            int consume = _SDecodeURIChar(buffer, length, value);
            length -= consume;
            buffer += consume;
        }

        // Got the name/value, set
        nameValueMap[name] = value;

        // Skip over the '&'
        --length;
        ++buffer;
    }

    // Valid if we have at some kind of path
    return !path.empty();
}

// --------------------------------------------------------------------------
void SComposeHTTP(string& buffer, const string& methodLine, const STable& nameValueMap, const string& content) {
    bool tryGzip = false;

    // Just walk across and compose a valid HTTP-like message
    buffer.clear();
    buffer += methodLine + "\r\n";
    for (pair<string, string> item : nameValueMap) {
        if (SIEquals("Set-Cookie", item.first)) {
            // Parse this list and generate a separate cookie for each.
            // Technically, this shouldn't be necessary: RFC2109 section 4.2.2
            // says cookies can be comma- delimited.  But it doesn't appear to
            // work in Firefox.
            list<string> cookieList;
            SParseList(item.second, cookieList, S_COOKIE_SEPARATOR); // A bit of a hack, yuck
            for (string& cookie : cookieList) {
                buffer += "Set-Cookie: " + cookie + "\r\n";
            }
        } else if (SIEquals("Content-Length", item.first)) {
            // Ignore Content-Length; will be generated fresh later
        } else if (SIEquals("Content-Encoding", item.first) && SIEquals("gzip", item.second)) {
            tryGzip = !content.empty();
        } else {
            buffer += item.first + ": " + SEscape(item.second, "\r\n\t") + "\r\n";
        }
    }

    const string gzipContent = tryGzip ? SGZip(content) : "";
    const bool gzipSuccess = !gzipContent.empty();
    const string& finalContent = gzipSuccess ? gzipContent : content;

    if (gzipSuccess) {
        buffer += "Content-Encoding: gzip\r\n";
    }

    // Always add a Content-Length, even if no content, so there is no ambiguity
    buffer += "Content-Length: " + SToStr(finalContent.size()) + "\r\n";

    // Finish the message and add the content, if any
    buffer += "\r\n";
    buffer += finalContent;
}

// --------------------------------------------------------------------------
string SComposePOST(const STable& nameValueMap) {
    // Accumulate and convert
    ostringstream out;
    for (pair<string, string> item : nameValueMap) {
        // Output the name and value, if any.  If the value is actually a
        // separated list of values, re-add the name each time
        if (item.second.empty()) {
            // No value, just add without
            out << SEncodeURIComponent(item.first) << "=&";
        } else {
            // Add as many times as there are values
            list<string> valueList;
            SParseList(item.second, valueList, S_COOKIE_SEPARATOR);
            for (string& value : valueList) {
                out << SEncodeURIComponent(item.first) << "=" << SEncodeURIComponent(value) << "&";
            }
        }
    }
    string outStr = out.str();
    outStr.resize(outStr.size() - 1); // Trim off trailing '&'
    return outStr;
}

// --------------------------------------------------------------------------
bool SParseHost(const string& host, string& domain, uint16_t& port) {
    // Split around the ':'
    domain = SBefore(host, ":");
    const string& portStr = SAfter(host, ":");
    if (domain.empty() || portStr.empty())
        return false; // Invalid host

    // Make sure the second part is a valid 16-bit host
    int portInt = atoi(portStr.c_str());
    if (portInt < 0 || portInt > 65535)
        return false; // Invalid port

    // Downcast and return success
    port = (uint16_t)portInt;
    return true;
}

// --------------------------------------------------------------------------
string SEncodeURIComponent(const string& value) {
    // Construct an encoded version.  According to:
    // http://developer.mozilla.org/en/docs/Core_JavaScript_1.5_Reference:Global_Functions:encodeURIComponent
    // it "escapes all characters except the following: alphabetic, decimal digits, - _ . ! ~ * ' ( )"
    const char* hexChars = "0123456789ABCDEF";
    string working;
    for (int c = 0; c < (int)value.size(); ++c) {
        // Test this character
        char ch = value[c];
        // Why isn't this just isalnum(ch)?
        // http://cplusplus.com/reference/clibrary/cctype/isalnum/
        if (SWITHIN('a', ch, 'z') || SWITHIN('A', ch, 'Z') || SWITHIN('0', ch, '9'))
            working += ch;
        else
            switch (ch) {
            case ' ':
                // Unsafe character, replace
                working += '+';
                break;

            case '-':
            case '_':
            case '.':
            case '!':
            case '~':
            case '*':
            case '(':
            case ')':
                // Safe character
                working += ch;
                break;

            default:
                // Unsafe character, escape
                working += '%';
                working += hexChars[ch >> 4];
                working += hexChars[ch & 0xF];
                break;
            }
    }

    // Done
    return working;
}

// --------------------------------------------------------------------------
string SDecodeURIComponent(const char* buffer, int length) {
    // Walk across and decode
    string working;
    while (length > 0) {
        // Consume some characters
        int consume = _SDecodeURIChar(buffer, length, working);
        length -= consume;
        buffer += consume;
    }
    return working;
}

// --------------------------------------------------------------------------
extern const char* _SParseJSONValue(const char* ptr, const char* end, string& value, bool populateValue);

string SToJSON(const int64_t value, const bool forceString) {
    return SToJSON(to_string(value), forceString);
}

string SToJSON(const string& value, const bool forceString) {
    // Is it an integer?
    if (SToStr(SToInt64(value.c_str())) == value) {
        return value;
    }
    // Is it a float?
    if (SToStr(SToFloat(value.c_str())) == value) {
        return value;
    }

    // Is it boolean?
    if (SIEquals(value, "true"))
        return "true";
    if (SIEquals(value, "false"))
        return "false";

    // Is it null?
    if (SIEquals(value, "null"))
        return "null";

    // Is it already a JSON array or object?
    if (!forceString && value.size() >= 2 &&
        ((value[0] == '[' && value[value.size() - 1] == ']') || (value[0] == '{' && value[value.size() - 1] == '}'))) {
        // If we can parse it, then return the array or object.
        string ignore;
        const char* ptr = value.c_str();
        const char* end = ptr + value.size();
        const char* parseEnd = _SParseJSONValue(ptr, end, ignore, false);
        if (parseEnd == end) // Parsed it all.
            return value;
    }

    // Otherwise, it's a string -- escape and return
    // We need to escape all control characters in the string, not just the white-space control characters.
    return "\"" + SEscape(value,     "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"
                                 "\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f\"\\/",
                          '\\') +
           "\"";
}

// --------------------------------------------------------------------------
string SComposeJSONObject(const STable& nameValueMap, const bool forceString) {
    if (nameValueMap.empty())
        return "{}";
    string working = "{";
    for (pair<string, string> item : nameValueMap) {
        working += "\"" + item.first + "\":" + SToJSON(item.second, forceString) + ",";
    }
    working.resize(working.size() - 1);
    working += "}";
    return working;
}

// --------------------------------------------------------------------------
#define _JSONWS()                                                                                                      \
    do {                                                                                                               \
        while (ptr < end && isspace(*ptr))                                                                             \
            ++ptr;                                                                                                     \
        if (ptr >= end)                                                                                                \
            return ptr;                                                                                                \
    } while (0)
#define _JSONTEST(_CH_)                                                                                                \
    do {                                                                                                               \
        if (ptr >= end || *ptr != _CH_) {                                                                              \
            if (ptr >= end)                                                                                            \
                SDEBUG("Expecting: '" << _CH_ << "', found 'eol'");                                                    \
            else                                                                                                       \
                SDEBUG("Expecting: '" << _CH_ << "', found '" << ptr << "'");                                          \
            return NULL;                                                                                               \
        }                                                                                                              \
        ++ptr;                                                                                                         \
    } while (0)
#define _JSONASSERTPTR()                                                                                               \
    do {                                                                                                               \
        if (ptr == NULL)                                                                                               \
            return NULL;                                                                                               \
    } while (0)
//#define _JSONLOG( ) do { cout << __LINE__ << ": " << ptr << endl; } while(0)
#define _JSONLOG()                                                                                                     \
    do {                                                                                                               \
    } while (0)
const char* _SParseJSONString(const char* ptr, const char* end, string& out, bool populateOut) {
    SASSERT(ptr && end);
    SASSERT(*ptr);
    _JSONLOG();
    // Walk across and find the end quote
    _JSONWS();
    _JSONTEST('"');
    const char* strStart = ptr;
    for (; ptr < end && *ptr; ++ptr) {
        // Found the end of this string
        if (*ptr == '"')
            break;

        // We want to skip all escaped characters so we don't mistakenly count
        // an escaped double-quote as the actual end.
        else if (*ptr == '\\')
            ++ptr;
    }
    _JSONTEST('"');

    if (populateOut) {
        string strOut(strStart, ptr - strStart - 1);
        out += SUnescape(strOut.c_str(), '\\');
    }
    return ptr;
}

// --------------------------------------------------------------------------
const char* _SParseJSONArray(const char* ptr, const char* end, list<string>& out, bool populateOut) {
    SASSERT(ptr && end);
    SASSERT(*ptr);
    _JSONLOG();
    // Walk across the array
    _JSONWS();
    _JSONTEST('[');
    _JSONWS();
    if (*ptr == ']')
        return ptr + 1; // Empty array
    while (true) {
        // Find the value
        _JSONWS();
        string value;
        ptr = _SParseJSONValue(ptr, end, value, populateOut);
        _JSONASSERTPTR(); // Make sure no parse error.
        if (populateOut)
            out.push_back(value);
        _JSONLOG();

        // See if we're done
        _JSONWS();
        if (*ptr == ']')
            return ptr + 1; // Done
        _JSONTEST(',');
    }
}

// --------------------------------------------------------------------------
const char* _SParseJSONObject(const char* ptr, const char* end, STable& out, bool populateOut) {
    SASSERT(ptr && end);
    SASSERT(*ptr);
    _JSONLOG();
    // Walk across the name value table
    _JSONWS();
    _JSONTEST('{');
    _JSONWS();
    if (*ptr == '}')
        return ptr + 1; // Empty object
    while (true) {
        // Find the name
        _JSONWS();
        string name;
        ptr = _SParseJSONString(ptr, end, name, populateOut);
        _JSONASSERTPTR(); // Make sure no parse error.
        _JSONWS();
        _JSONTEST(':');

        // Find the value
        _JSONWS();
        string value;
        ptr = _SParseJSONValue(ptr, end, value, populateOut);
        _JSONASSERTPTR(); // Make sure no parse error.
        if (populateOut) {
            // Got one more
            out[name] = value;
        }
        _JSONLOG();

        // See if we're done
        _JSONWS();
        if (*ptr == '}')
            return ptr + 1; // Finished this object
        _JSONTEST(',');
    }
}

// --------------------------------------------------------------------------
const char* _SParseJSONValue(const char* ptr, const char* end, string& value, bool populateValue) {
    _JSONLOG();
    // Classify based on the first character
    _JSONWS();
    switch (*ptr) {
    case '"': {
        // String
        ptr = _SParseJSONString(ptr, end, value, populateValue);
        _JSONASSERTPTR(); // Make sure no parse error.
        break;
    }

    case '{': {
        // Object -- just grab the string representation.
        STable ignore;
        const char* valueStart = ptr;
        ptr = _SParseJSONObject(ptr, end, ignore, false);
        _JSONASSERTPTR(); // Make sure no parse error.
        if (populateValue) {
            value.resize(ptr - valueStart);
            memcpy(&value[0], valueStart, ptr - valueStart);
        }
        break;
    }

    case '[': {
        // Array -- just grab the string representation.
        list<string> ignore;
        const char* valueStart = ptr;
        ptr = _SParseJSONArray(ptr, end, ignore, false);
        _JSONASSERTPTR(); // Make sure no parse error.
        if (populateValue) {
            value.resize(ptr - valueStart);
            memcpy(&value[0], valueStart, ptr - valueStart);
        }
        break;
    }

    default: {
        // Maybe a number?
        if (isdigit(*ptr) || (*ptr == '-' && ptr + 1 < end && isdigit(*(ptr + 1)))) {
            // Parse this number
            const char* numStart = ptr;

            // Maybe a negative value?
            if (*ptr == '-')
                ++ptr;
            while (ptr < end && isdigit(*ptr))
                ++ptr;

            // Maybe a float value?
            if (*ptr == '.') {
                ++ptr;
                while (ptr < end && isdigit(*ptr))
                    ++ptr;
            }

            // Maybe a scientific notation value?
            if (*ptr == 'e' || *ptr == 'E') {
                ++ptr;
                if (*ptr == '-' || *ptr == '+')
                    ++ptr;
                while (ptr < end && isdigit(*ptr))
                    ++ptr;
            }

            if (populateValue) {
                value.resize(ptr - numStart);
                memcpy(&value[0], numStart, ptr - numStart);
            }
        } else if (!strncmp(ptr, "true", 4)) {
            // Found boolean true
            if (populateValue)
                value = "true";
            ptr += 4; // strlen(true)
        } else if (!strncmp(ptr, "false", 5)) {
            // Found boolean false
            if (populateValue)
                value = "false";
            ptr += 5; // strlen(false)
        } else if (!strncmp(ptr, "null", 4)) {
            // Found null
            if (populateValue)
                value = "null";
            ptr += 4; // strlen(null)
        }
        // else unsupported, ignore
        break;
    }
    }

    // Done
    return ptr;
}

STable SParseJSONObject(const string& object) {
    // Assume it's an object
    STable out;
    if (object.size() < 2)
        return out;
    const char* ptr = object.c_str();
    const char* end = ptr + object.size();
    const char* parseEnd = _SParseJSONObject(ptr, end, out, true);

    // Trim trailing whitespace
    while (parseEnd && parseEnd < end && *parseEnd && isspace(*parseEnd))
        ++parseEnd;

    // Did we parse it all?  If not, return nothing.
    if (parseEnd < end) {
        // Did not parse it all.
        if (parseEnd) {
            SWARN("Incomplete parse at:" << parseEnd << "(" << (int)(end - parseEnd) << ", ch:" << (int)(*parseEnd)
                                         << ")");
        } else {
            SWARN("Malformed JSON (" << out.size() << " entries parsed)");
        }
        return STable();
    }
    return out;
}

// --------------------------------------------------------------------------
list<string> SParseJSONArray(const string& array) {
    // Assume it's an array
    list<string> out;
    if (array.size() < 2)
        return out;
    const char* ptr = array.c_str();
    const char* end = ptr + array.size();
    const char* parseEnd = _SParseJSONArray(ptr, end, out, true);
    if (parseEnd != end) // Did not parse it all.
        return list<string>();
    return out;
}

// --------------------------------------------------------------------------
string SGZip(const string& content) {
    z_stream stream;

    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    stream.next_in = (unsigned char*)content.c_str();
    stream.avail_in = (unsigned int)content.size();

    int GZIP_ENCODING = 16;

    unsigned int bufferSize = stream.avail_in + (stream.avail_in / 1000) + 20;
    unsigned char* outBuffer = new unsigned char[bufferSize];

    stream.avail_out = bufferSize;
    stream.next_out = outBuffer;

    int status = deflateInit2(&stream, Z_BEST_COMPRESSION, Z_DEFLATED, MAX_WBITS | GZIP_ENCODING, MAX_MEM_LEVEL,
                              Z_DEFAULT_STRATEGY);

    if (status != Z_OK) {
        SHMMM("failed to initialize a GZip context");
        return "";
    }

    status = deflate(&stream, Z_FINISH);
    if (status != Z_STREAM_END && status != Z_OK) {
        SHMMM("We deflated but we didn't get Z_STREAM_END or Z_OK, we got " << status);
        deflateEnd(&stream);
        if (status == Z_OK) {
            status = Z_BUF_ERROR;
        }
    } else {
        status = deflateEnd(&stream);
    }

    string result;
    result.append((char*)outBuffer, stream.total_out);

    delete[] outBuffer;

    if (status == Z_OK) {
        return result;
    } else {
        SHMMM("GZip operation failed status:" << status);
        return "";
    }
}

string SGUnzip (const string& content) {
    int CHUNK = 16384;
    int status;
    unsigned have;
    z_stream strm;
    unsigned char out[CHUNK];
    string data;

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;

    status = inflateInit2(&strm, 16 + MAX_WBITS);
    if (status != Z_OK) {
        SWARN("Error inflating stream for gunzip, status: " << status);
        return "";
    }

    strm.avail_in = content.size();
    strm.next_in = (unsigned char*)content.c_str();

    do {
        strm.avail_out = CHUNK;
        strm.next_out = out;
        status = inflate(&strm, Z_NO_FLUSH);
        switch (status) {
            case Z_NEED_DICT:
            case Z_DATA_ERROR:
            case Z_MEM_ERROR:
                inflateEnd(&strm);
                SWARN("Error gunzipping, status:" << status);
                return "";
        }
        have = CHUNK - strm.avail_out;
        data.append((char*)out, have);
    } while (strm.avail_out == 0);

    status = inflateEnd(&strm);
    if (status != Z_OK) {
        SWARN("Error gunzipping, status: " << status);
        return "";
    }

    return data;
}

/////////////////////////////////////////////////////////////////////////////
// Socket helpers
/////////////////////////////////////////////////////////////////////////////

// --------------------------------------------------------------------------
int S_socket(const string& host, bool isTCP, bool isPort, bool isBlocking) {
    // Try to set up the socket
    int s = 0;
    try {
        // First, just parse the host
        string domain;
        uint16_t port = 0;
        if (!SParseHost(host, domain, port)) {
            STHROW("invalid host: " + host);
        }

        // Is the domain just a raw IP?
        unsigned int ip = inet_addr(domain.c_str());
        if (!ip || ip == INADDR_NONE) {
            // Nope -- resolve the domain
            uint64_t start = STimeNow();

            // Allocate and initialize addrinfo structures.
            struct addrinfo hints;
            memset(&hints, 0, sizeof hints);
            struct addrinfo* resolved = nullptr;

            // Set up the hints.
            hints.ai_family = AF_INET; // IPv4
            hints.ai_socktype = SOCK_STREAM;

            // Do the initialization.
            int result = getaddrinfo(domain.c_str(), to_string(port).c_str(), &hints, &resolved);
            SINFO("DNS lookup took " << (STimeNow() - start) / 1000 << "ms for '" << domain << "'.");

            // There was a problem.
            if (result || !resolved) {
                freeaddrinfo(resolved);
                STHROW("can't resolve host error no#" + result);
            }
            // Grab the resolved address.
            sockaddr_in* addr = (sockaddr_in*)resolved->ai_addr;
            ip = addr->sin_addr.s_addr;
            char plainTextIP[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &addr->sin_addr, plainTextIP, INET_ADDRSTRLEN);
            SINFO("Resolved " << domain << " to ip: " << plainTextIP << ".");

            // Done resolving.
            freeaddrinfo(resolved);
        }

        // Open a socket
        if (isTCP)
            s = (int)socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        else
            s = (int)socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (!s || s == -1)
            STHROW("couldn't open");

        // Enable non-blocking, if requested
        if (!isBlocking) {
            // Set non-blocking
            int flags = fcntl(s, F_GETFL);
            if ((flags < 0) || fcntl(s, F_SETFL, flags | O_NONBLOCK))
                STHROW("couldn't set non-blocking");
        }

        // If this is a port, bind
        if (isPort) {
            // Enable port reuse (so we don't have TIME_WAIT binding issues) and
            u_long enable = 1;
            if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char*)&enable, sizeof(enable)))
                STHROW("couldn't set REUSEADDR");

            // Bind to the configured port
            sockaddr_in addr;
            memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            addr.sin_addr.s_addr = ip;
            if (::bind(s, (sockaddr*)&addr, sizeof(addr))) {
                STHROW("couldn't bind");
            }

            // Start listening, if TCP
            if (isTCP && listen(s, SOMAXCONN))
                STHROW("couldn't listen");
        } else {
            // If TCP, connect
            sockaddr_in addr;
            memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            addr.sin_addr.s_addr = ip;
            if (connect(s, (sockaddr*)&addr, sizeof(addr)) == -1)
                switch (S_errno) {
                case S_EWOULDBLOCK:
                case S_EALREADY:
                case S_EINPROGRESS:
                case S_EINTR:
                case S_EISCONN:
                    // Not fatal, ignore
                    break;

                default:
                    STHROW("couldn't connect");
                }
        }

        // Success, ready to go.
        return s;
    } catch (const SException& e) {
        // Failed to open
        SWARN("Failed to open " << (isTCP ? "TCP" : "UDP") << (isPort ? " port" : " socket") << " '" << host
                                << "': " << e.what() << "(errno=" << S_errno << " '" << strerror(S_errno) << "')");
        if (s > 0)
            close(s);
        return -1;
    }
}

// --------------------------------------------------------------------------
ssize_t S_recvfrom(int s, char* recvBuffer, int recvBufferSize, sockaddr_in& fromAddr) {
    SASSERT(s);
    SASSERT(recvBuffer);
    SASSERT(recvBufferSize > 0);
    // Try to receive into the buffer
    socklen_t fromAddrLen = sizeof(fromAddr);
    memset(&fromAddr, 0, sizeof(fromAddr));
    ssize_t numRecv = recvfrom(s, recvBuffer, recvBufferSize - 1, 0, (sockaddr*)&fromAddr, &fromAddrLen);
    recvBuffer[numRecv] = 0;

    // Process the result
    if (numRecv == 0) {
        // This shouldn't happen
        SWARN("recvfrom(" << fromAddr << ") failed with graceful shutdown on UDP port, closing.");
        return -1; // Request close
    } else if (numRecv < 0) {
        // Some kind of error -- what happened?
        switch (S_errno) {
        case S_NOTINITIALISED:
        case S_ENETDOWN:
        case S_EFAULT:
        case S_ENETRESET:
        case S_EISCONN:
        case S_ENOTSOCK:
        case S_EOPNOTSUPP:
        case S_EINVAL:
        case S_ETIMEDOUT:
        case S_ECONNRESET:
            // Interesting, reset the port and hope it clears.
            // **FIXME: Handle ICMP responses
            SWARN("recvfrom(" << fromAddr << ") failed with response '" << strerror(S_errno) << "' (#" << S_errno
                              << "), closing.");
            return -1; // Request close

        case S_EINTR:
        case S_EINPROGRESS:
        case S_EWOULDBLOCK:
        case S_ESHUTDOWN:
            // Not interesting, and not fatal.
            return 0;

        case S_EMSGSIZE:
        default:
            // Interesting, this shouldn't happen
            SWARN("recvfrom(" << fromAddr << ") failed with response '" << strerror(S_errno) << "' (#" << S_errno
                              << "), ignoring.");
            return 0;
        }
    } else {
        // Received data; good to go
        return numRecv;
    }
}

// --------------------------------------------------------------------------
int S_accept(int port, sockaddr_in& fromAddr, bool isBlocking) {
    // Try to receive into the buffer
    socklen_t fromAddrLen = sizeof(fromAddr);
    memset(&fromAddr, 0, sizeof(fromAddr));
    int s = (int)accept(port, (sockaddr*)&fromAddr, &fromAddrLen);

    // Process the result
    if (s != -1) {
        // Enable non-blocking, if requested.
        if (!isBlocking) {
            // Set non-blocking
            int flags = fcntl(s, F_GETFL);
            if ((flags < 0) || fcntl(s, F_SETFL, flags | O_NONBLOCK))
                STHROW("couldn't set non-blocking");
        }

        // Accepted a valid socket; return
        return s;
    } else {
        // Some kind of error -- what happened?
        switch (S_errno) {
        case S_NOTINITIALISED:
        case S_EFAULT:
        case S_EINVAL:
        case S_EMFILE:
        case S_ENETDOWN:
        case S_ENOBUFS:
        case S_ENOTSOCK:
        case S_EOPNOTSUPP:
            // Interesting; reset the port and hope it clears.
            SWARN("accept() failed with response '" << strerror(S_errno) << "' (#" << S_errno << ") from '" << fromAddr
                                                    << "', closing.");
            return -1; // Request close of the socket

        case S_ECONNRESET:
        default:
            // Interesting, but non-fatal
            SWARN("accept() failed with response '" << strerror(S_errno) << "' (#" << S_errno << ") from '" << fromAddr
                                                    << "', ignoring.");
            return 0; // Nothing more to accept this loop

        case S_EINTR:
        case S_EINPROGRESS:
        case S_EWOULDBLOCK:
            // Not interesting, and not fatal.
            return 0; // Nothing more to accept this loop
        }
    }
}

bool SCheckNetworkErrorType(const string& logPrefix, const string& peer, int errornumber) {
    switch (errornumber) {
        // These are only interesting enough for an info line.
        case S_ECONNABORTED:
        case S_ETIMEDOUT:
        case S_ENOTCONN:
        case S_ECONNREFUSED:
        case S_ECONNRESET:
        case S_EPIPE:
            SHMMM(logPrefix << "(" << peer << ") failed with response '" << strerror(errornumber) << "' (#" << errornumber << "), closing.");
            return false; // Socket died

        // And these aren't interesting enough to say anything about at all (and aren't fatal).
        case S_EINTR:
        case S_EINPROGRESS:
        case S_ESHUTDOWN:
        case S_EWOULDBLOCK: // Same as S_EAGAIN in Linux.
            return true; // Socket still alive

        // Anything else is interesting enough to warn.
        default:
            SWARN(logPrefix << "(" << peer << ") failed with response '" << strerror(errornumber) << "' (#" << errornumber << "), closing.");
            return false; // Socket died
    }
}

// --------------------------------------------------------------------------
// Receives data from a socket and appends to a string.  Returns 'true' if
// the socket is still alive when done.
bool S_recvappend(int s, SFastBuffer& recvBuffer) {
    SASSERT(s);
    // Figure out if this socket is blocking or non-blocking
    int flags = fcntl(s, F_GETFL);
    bool blocking = !(flags & O_NONBLOCK);

    // Log size of the buffer before we read from it.
    int bytesInBuffer = 0;
    int ret = 0;
    ret = ioctl(s, FIONREAD, &bytesInBuffer);

    if (ret < 0) {
        SHMMM("Unable to get length of socket buffer error: " << strerror(S_errno));
    }

    // Keep trying to receive as long as we can
    char buffer[4096];
    int totalRecv = 0;
    ssize_t numRecv = 0;
    sockaddr_in fromAddr;
    socklen_t fromAddrLen = sizeof(fromAddr);
    while ((numRecv = recvfrom(s, buffer, sizeof(buffer), 0, (sockaddr*)&fromAddr, &fromAddrLen)) > 0) {
        // Got some more data
        recvBuffer.append(buffer, numRecv);
        totalRecv += numRecv;

        // If this is a blocking socket, don't try again, once is enough
        if (blocking) {
            return true; // We're still alive
        }
    }

    // See how we finished
    if (numRecv == 0) {
        return false; // Graceful shutdown; socket closed
    }
    // Some kind of error -- what happened?
    stringstream addrStr;
    addrStr << fromAddr;
    return SCheckNetworkErrorType("recv", addrStr.str(), S_errno);
}

// --------------------------------------------------------------------------
bool S_sendconsume(int s, SFastBuffer& sendBuffer) {
    SASSERT(s);
    // If empty, nothing to do
    if (sendBuffer.empty()) {
        return true; // Assume no error, still alive
    }

    // 17 is size of "ESCALATE_RESPONSE".
    if (SStartsWith(sendBuffer.c_str(), sendBuffer.size(), "ESCALATE_RESPONSE", 17)) {
        SData tempData;
        tempData.deserialize(sendBuffer);
        string id = tempData["id"];
        SINFO("Sending an ESCALATE_RESPONSE for id " << id);
    }

    // Send as much as we can
    ssize_t numSent = send(s, sendBuffer.c_str(), sendBuffer.size(), MSG_NOSIGNAL);
    string errorMessage;
    if (numSent == -1) {
        errorMessage = " Error: "s + strerror(errno);
    }

    if (numSent > 0) {
        sendBuffer.consumeFront(numSent);
    }

    // Exit if no error
    if (numSent >= 0) {
        return true; // No error; still alive
    }

    // If we failed to send with over 1GB in the buffer, return false, even if the error would normally be non-fatal.
    if (sendBuffer.size() > 1024 * 1024 * 1024) {
        SWARN("send() failed with response '" << strerror(errno) << "' (#" << errno << "), and buffer size: "
              << sendBuffer.size() << ", closing.");
        return false;
    }

    // Error, what kind?
    return SCheckNetworkErrorType("send", SGetPeerName(s), S_errno);
}

void SFDset(fd_map& fdm, int socket, short evts) {
    fd_map::iterator existing = fdm.find(socket);
    if (existing != fdm.end()) {
        existing->second.events = evts | existing->second.events;
    } else {
        fdm[socket] = (pollfd){socket, evts, 0};
    }
}

bool SFDAnySet(fd_map& fdm, int socket, short evts) {
    if (evts == 0) {
        return false;
    }
    if (fdm.find(socket) == fdm.end()) {
        return false;
    }
    pollfd& fd = fdm[socket];
    return fd.revents & evts;
}

// --------------------------------------------------------------------------
int S_poll(fd_map& fdm, uint64_t timeout) {
    // Why doesn't this function lock around our fd_map, you might ask? Because in the existing bedrock architecture,
    // each worker thread allocates its own fd_map, and thus different threads wont compete for the same resource
    // here. The only place they share resources is around a bedrock MessageQueue, which does its own locking. If we
    // ever want to allow multiple threads to manipulate a shared fd_map directly, then we need locking in the related
    // functions.

    // Build a vector we can use to pass data to poll().
    vector<pollfd> pollvec;
    for (pair<int, pollfd> pfd : fdm) {
        pollvec.push_back(pfd.second);
    }

    // Timeout is specified in microseconds, but poll uses milliseconds, so we divide by 1000.
    int timeoutVal = int(timeout / 1000);
    int returnValue = poll(&pollvec[0], fdm.size(), timeoutVal);

    // And write our returned events back to our original structure.
    for (pollfd pfd : pollvec) {
        fdm[pfd.fd].revents = pfd.revents;
    }

    if (returnValue == -1) {
        SWARN("Poll failed with response '" << strerror(S_errno) << "' (#" << S_errno << "), ignoring");
    }
    return returnValue;
}

/////////////////////////////////////////////////////////////////////////////
// Network helpers
/////////////////////////////////////////////////////////////////////////////
// --------------------------------------------------------------------------
string SGetHostName() {
    // Simple enough
    char hostname[1024];
    gethostname(hostname, sizeof(hostname));
    return string(hostname);
}

// --------------------------------------------------------------------------
string SGetPeerName(int s) {
    // Just call the function that does this
    sockaddr_in addr{};
    socklen_t socklen = sizeof(addr);
    int result = getpeername(s, (sockaddr*)&addr, &socklen);
    if (result == 0) {
        return SToStr(addr);
    } else {
        return "(errno#" + SToStr(S_errno) + ")";
    }
}

// --------------------------------------------------------------------------
string SAESEncrypt(const string& buffer, const string& ivStr, const string& key) {
    SASSERT(key.size() == SAES_KEY_SIZE);
    // Pad the buffer to land on SAES_BLOCK_SIZE boundary (required).
    string paddedBuffer = buffer;
    if (buffer.size() % SAES_BLOCK_SIZE != 0) {
        paddedBuffer.append(SAES_BLOCK_SIZE - ((int)buffer.size() % SAES_BLOCK_SIZE), (char)0);
    }

    // Encrypt
    unsigned char iv[SAES_BLOCK_SIZE];
    memcpy(iv, ivStr.c_str(), SAES_BLOCK_SIZE);
    mbedtls_aes_context ctx;
    mbedtls_aes_setkey_enc(&ctx, (unsigned char*)key.c_str(), 8 * SAES_KEY_SIZE);
    string encryptedBuffer;
    encryptedBuffer.resize(paddedBuffer.size());
    mbedtls_aes_crypt_cbc(&ctx, MBEDTLS_AES_ENCRYPT, (int)paddedBuffer.size(), iv, (unsigned char*)paddedBuffer.c_str(),
                          (unsigned char*)encryptedBuffer.c_str());

    return encryptedBuffer;
}


// --------------------------------------------------------------------------
string SAESDecrypt(const string& buffer, unsigned char* iv, const string& key) {
    string decryptedBuffer = SAESDecryptNoStrip(buffer, buffer.size(), iv, key);

    // Trim off the padding.
    int size = (int)decryptedBuffer.find('\0');
    if (size != (int)string::npos) {
        decryptedBuffer.resize(size);
    }

    return decryptedBuffer;
}

string SAESDecrypt(const string& buffer, const string& ivStr, const string& key) {
    SASSERT(ivStr.size() == SAES_IV_SIZE);
    unsigned char iv[SAES_IV_SIZE];
    memcpy(iv, ivStr.c_str(), SAES_IV_SIZE);
    return SAESDecrypt(buffer, iv, key);
}

// These decrypt functions are used to return a value that still includes possible
// padding, so it is up the caller to manage stripping the potential NULL chars off the end.
string SAESDecryptNoStrip(const string& buffer, const size_t& bufferSize, unsigned char* iv, const string& key) {
    SASSERT(key.size() == SAES_KEY_SIZE);
    // If the message is invalid.
    if (buffer.size() % SAES_BLOCK_SIZE != 0) {
        return "";
    }

    // Decrypt
    mbedtls_aes_context ctx;
    string decryptedBuffer;
    decryptedBuffer.resize(bufferSize);
    mbedtls_aes_setkey_dec(&ctx, (unsigned char*)key.c_str(), 8 * SAES_KEY_SIZE);
    mbedtls_aes_crypt_cbc(&ctx, MBEDTLS_AES_DECRYPT, (int)buffer.size(), iv, (unsigned char*)buffer.c_str(),
                          (unsigned char*)decryptedBuffer.c_str());
    return decryptedBuffer;


}

string SAESDecryptNoStrip(const string& buffer, const size_t& bufferSize, const string& ivStr, const string& key) {
    SASSERT(ivStr.size() == SAES_IV_SIZE);
    unsigned char iv[SAES_IV_SIZE];
    memcpy(iv, ivStr.c_str(), SAES_IV_SIZE);
    return SAESDecryptNoStrip(buffer, bufferSize, iv, key);
}

/////////////////////////////////////////////////////////////////////////////
// File stuff
/////////////////////////////////////////////////////////////////////////////

// --------------------------------------------------------------------------
bool SFileExists(const string& path) {
    // Return true if it exists and is a file
    struct stat out;
    if (stat(path.c_str(), &out) != 0) {
        return false;
    }
    return (out.st_mode & S_IFREG) != 0;
}

// --------------------------------------------------------------------------
bool SFileLoad(const string& path, string& buffer) {
    // Initialize the output
    buffer.clear();

    // Try to open the file
    FILE* fp = fopen(path.c_str(), "rb");
    if (!fp)
        return false; // Couldn't open

    // Read as much as we can
    char readBuffer[32 * 1024];
    size_t numRead = 0;
    while ((numRead = fread(readBuffer, 1, sizeof(readBuffer), fp))) {
        // Append to the buffer
        size_t oldSize = buffer.size();
        buffer.resize(oldSize + numRead);
        memcpy(&buffer[oldSize], readBuffer, numRead);
    }

    // Done
    fclose(fp);
    return true; // Success
}

// --------------------------------------------------------------------------
string SFileLoad(const string& path) {
    string buffer;
    SFileLoad(path, buffer);
    return buffer;
}

// --------------------------------------------------------------------------
bool SFileSave(const string& path, const string& buffer) {
    // Try to open the file
    FILE* fp = fopen(path.c_str(), "wb");
    if (!fp)
        return false; // Couldn't open

    // Write to disk
    size_t numWritten = fwrite(buffer.c_str(), 1, buffer.size(), fp);
    fclose(fp);
    if (numWritten == buffer.size())
        return true; // Success

    // Couldn't write entirely, delete and fail
    unlink(path.c_str());
    return false; // Failed
}

// --------------------------------------------------------------------------
bool SFileCopy(const string& fromPath, const string& toPath) {
    // Figure out the size of the file we're copying.
    uint64_t fromSize = SFileSize(fromPath);
    if (!fromSize) {
        SWARN("File " << fromPath << " is empty! Copying anyway.");
    }

    // Open both the source and destination files.
    FILE* from = fopen(fromPath.c_str(), "rb");
    if (!from) {
        SWARN("Couldn't open file " << fromPath << " for reading. Error: " << errno << ", " << strerror(errno) << ".");
        return false;
    }
    SINFO("Successfully opened " << fromPath << " for reading.");
    FILE* to = fopen(toPath.c_str(), "wb");
    if (!to) {
        SWARN("Couldn't open file " << toPath << " for writing. Error: " << errno << ", " << strerror(errno) << ".");
        fclose(from);
        return false;
    }
    SINFO("Successfully opened " << toPath << " for writing.");
    bool success = false;
    try {
        // Read and write
        char buf[1024 * 64];
        size_t numRead = 0;
        uint64_t completeBytes = 0;
        int completePercent = 0;
        bool readAny = false;
        bool writtenAny = false;
        while ((numRead = fread(buf, 1, sizeof(buf), from)) > 0) {
            if (!readAny) {
                readAny = true;
                SINFO("Read first " << numRead << " bytes from " << fromPath << ".");
            }
            if (fwrite(buf, 1, numRead, to) != numRead) {
                SWARN("Failure writing to " << toPath << " Error: " << errno << ", " << strerror(errno) << ".");
                STHROW("write error");
            } else {
                if (!writtenAny) {
                    writtenAny = true;
                    SINFO("Wrote first " << numRead << " bytes to " << toPath << ".");
                }
                completeBytes += numRead;
                int percent = fromSize ? ((completeBytes * 100) / fromSize) : 0;
                if (percent > completePercent) {
                    SINFO("Copying " << fromPath << " to " << toPath << " is " << percent << "% complete.");
                    completePercent = percent;
                }
            }
        }

        // See if we failed, or if we hit EOF.
        if (ferror(from)) {
            SWARN("Failure reading from " << fromPath << " Error: " << errno << ", " << strerror(errno) << ".");
        } else {
            // If there was no error, we should be at the end of the file.
            if (!feof(from)) {
                SWARN("Done reading from " << fromPath << " with no error, but not EOF.");
            }
        }

        // Done
        success = true;
    } catch (const SException& e) {
        // Problem
        SWARN("Failed copying file '" << fromPath << "' to '" << toPath << "' (" << e.what() << ")");
    }
    fclose(from);
    fclose(to);
    return success;
}

// --------------------------------------------------------------------------
bool SFileDelete(const string& path) {
    if (!SFileExists(path)) {
        return false;
    }

    const int result = unlink(path.c_str());
    if (result != 0) {
        SWARN("Failed deleting file '" << path << " code: " << result);
        return false;
    }
    return true;
}

// --------------------------------------------------------------------------
uint64_t SFileSize(const string& path) {
    struct stat out;
    if (stat(path.c_str(), &out)) {
        // Can't read
        return 0;
    }
    return out.st_size;
}

/////////////////////////////////////////////////////////////////////////////
// Cryptography stuff
/////////////////////////////////////////////////////////////////////////////

string SHashSHA1(const string& buffer) {
    string result;
    result.resize(20);
    mbedtls_sha1((unsigned char*)buffer.c_str(), buffer.size(), (unsigned char*)&result[0]);
    return result;
}

string SHashSHA256(const string& buffer) {
    string result;
    result.resize(32);
    mbedtls_sha256((unsigned char*)buffer.c_str(), buffer.size(), (unsigned char*)&result[0], 0);
    return result;
}

// --------------------------------------------------------------------------

string SEncodeBase64(const unsigned char* buffer, int size) {
    // First, get the required buffer size
    size_t olen = 0;
    mbedtls_base64_encode(0, 0, &olen, buffer, size);

    // Next, do the encode
    string out;
    out.resize(olen - 1); // -1 because trailing 0 is implied
    mbedtls_base64_encode((unsigned char*)&out[0], olen, &olen, buffer, size);
    return out;
}

string SEncodeBase64(const string& bufferString) {
    return SEncodeBase64((unsigned char*)bufferString.c_str(), bufferString.size());
}

// --------------------------------------------------------------------------
string SDecodeBase64(const unsigned char* buffer, int size) {
    // First, get the required buffer size
    size_t olen = 0;
    mbedtls_base64_decode(0, 0, &olen, buffer, size);

    // Next, do the decode
    string out;
    out.resize(olen);
    mbedtls_base64_decode((unsigned char*)&out[0], olen, &olen, buffer, size);
    return out;
}

string SDecodeBase64(const string& bufferString) {
    return SDecodeBase64((unsigned char*)bufferString.c_str(), bufferString.size());
}

// --------------------------------------------------------------------------
string SHMACSHA1(const string& key, const string& buffer) {
    // See: http://en.wikipedia.org/wiki/HMAC

    // First, build the secret pads
    int BLOCK_SIZE = 64;
    string ipadSecret(BLOCK_SIZE, 0x36), opadSecret(BLOCK_SIZE, 0x5c);
    for (int c = 0; c < (int)key.size(); ++c) {
        // XOR front of opadSecret/ipadSecret with secret access key
        ipadSecret[c] ^= key[c];
        opadSecret[c] ^= key[c];
    }

    // Then use it to make the hashes
    const string& innerHash = SHashSHA1(ipadSecret + buffer);
    const string& outerHash = SHashSHA1(opadSecret + innerHash);
    return outerHash;
}

// --------------------------------------------------------------------------
string SHMACSHA256(const string& key, const string& buffer) {
    // See: http://en.wikipedia.org/wiki/HMAC

    // First, build the secret pads
    int BLOCK_SIZE = 64;
    string ipadSecret(BLOCK_SIZE, 0x36), opadSecret(BLOCK_SIZE, 0x5c);
    for (int c = 0; c < (int)key.size(); ++c) {
        // XOR front of opadSecret/ipadSecret with secret access key
        ipadSecret[c] ^= key[c];
        opadSecret[c] ^= key[c];
    }

    // Then use it to make the hashes
    const string& innerHash = SHashSHA256(ipadSecret + buffer);
    const string& outerHash = SHashSHA256(opadSecret + innerHash);
    return outerHash;
}

/////////////////////////////////////////////////////////////////////////////
// SQLite Stuff
/////////////////////////////////////////////////////////////////////////////

// --------------------------------------------------------------------------
string SQList(const string& val, bool integersOnly) {
    // Parse and verify
    list<string> dirtyList;
    SParseList(val, dirtyList);
    list<string> cleanList;
    for (string& dirty : dirtyList) {
        // Make sure it's clean
        if (integersOnly) {
            const string& clean = SToStr(SToInt64(dirty));
            if (!clean.empty() && (clean == dirty))
                cleanList.push_back(clean);
        } else
            cleanList.push_back(SQ(dirty));
    }
    return SComposeList(cleanList);
}

// --------------------------------------------------------------------------
// Begins logging all queries to a logging database
FILE* _g_sQueryLogFP = nullptr;
extern void SQueryLogOpen(const string& logFilename) {
    // Make sure it's not already open
    if (_g_sQueryLogFP) {
        // Already open
        SHMMM("Attempting to open query log '" << logFilename << "' but a log is already open, ignoring.");
    } else {
        // Create a new logfile from scratch, replacing anything already there.
        // Note that we first open to a local variable so we can write the
        // schema line first, without another thread logging overtop of it
        SINFO("Opening query log '" << logFilename << "'");
        FILE* fp = fopen(logFilename.c_str(), "w");
        SASSERT(fp);
        const string& schema = "filename, query, elapsed\n";
        SASSERT(fwrite(schema.c_str(), 1, schema.size(), fp) == schema.size());

        // Assign to the global variable so other threads can start using it
        _g_sQueryLogFP = fp;
    }
}

// --------------------------------------------------------------------------
void SQueryLogClose() {
    // Is it even open?
    if (!_g_sQueryLogFP) {
        // Not open
        SHMMM("Trying to close query log but not open, ignoring.");
    } else {
        // Clear the global variable and wait a second, in case it's being called right now
        SINFO("Closing query log...");
        FILE* fp = _g_sQueryLogFP;
        _g_sQueryLogFP = nullptr;
        this_thread::sleep_for(chrono::seconds(1));

        // Close it
        fclose(fp);
        SINFO("Closed query log");
    }
}

// --------------------------------------------------------------------------
// Executes a SQLite query
int SQuery(sqlite3* db, const char* e, const string& sql, SQResult& result, int64_t warnThreshold, bool skipWarn) {
#define MAX_TRIES 3
    // Execute the query and get the results
    uint64_t startTime = STimeNow();
    int error = 0;
    int extErr = 0;

    size_t numLoops = 0;
    size_t prepareTimeUS = 0;
    size_t numSteps = 0;
    size_t stepTimeUS = 0;
    size_t longestStepTimeUS = 0;

    for (int tries = 0; tries < MAX_TRIES; tries++) {
        result.clear();
        SDEBUG(sql.substr(0, 20000));

        const char *statementRemainder = sql.c_str();
        do {
            numLoops++;
            sqlite3_stmt *preparedStatement = nullptr;
            size_t beforePrepare = 0;
            if (isSyncThread) {
                beforePrepare = STimeNow();
            }

            // sql.size() is the number of bytes in the string, excluding the null terminator.
            // statementRemainder points to the first unused byte in this string, meaning `statementRemainder - sql.c_str()` is the number of already-used bytes.
            // sql.size() minus the already-used bytes, is the number of bytes remaining, and we add one more for the null terminator.
            //
            // The null-terminator here is MASSIVELY IMPORTANT.
            // The docs here: https://www.sqlite.org/c3ref/prepare.html, indicate:
            // "there is a small performance advantage to passing an nByte parameter that is the number of bytes in the input string including the null-terminator."
            //
            // This is a massive understatement in some situations. Namely, this seems to avoid a call to strlen() on the whole string.
            // For long queries (say 100mb+), this can save 50ms per query. Further, if these queries are actually a large number of small queries concatenated together,
            // then this saves that 50ms for each of the smaller queries, which means those savings could be multiplied thousands of times.
            //
            // Calling strlen() or any function that iterates across the whole string here is a giant performance problem, and all the operations chosen here have
            // been picked specifically to avoid that.
            size_t maxLength = sql.size() - (statementRemainder - sql.c_str()) + 1;
            error = sqlite3_prepare_v2(db, statementRemainder, maxLength, &preparedStatement, &statementRemainder);
            if (isSyncThread) {
                prepareTimeUS += STimeNow() - beforePrepare;
            }
            if (error) {
                // Delete our statement.
                sqlite3_finalize(preparedStatement);

                // This will just drop through to the general error handling below.
                break;
            } else if (!preparedStatement) {
                // If we get a null statement (from parsing a blank string) we can skip, this isn't an error.
                error = SQLITE_OK;
                break;
            }
            int numColumns = sqlite3_column_count(preparedStatement);
            result.headers.resize(numColumns);

            while (true) {
                size_t beforeStep = 0;
                if (isSyncThread) {
                    beforeStep = STimeNow();
                }
                numSteps++;
                error = sqlite3_step(preparedStatement);
                if (isSyncThread) {
                    size_t stepTime = STimeNow() - beforeStep;
                    if (stepTime > longestStepTimeUS) {
                        longestStepTimeUS += stepTime;
                    }
                    stepTimeUS += stepTime;
                }

                for (int i = 0; i < numColumns; i++) {
                    result.headers[i] = sqlite3_column_name(preparedStatement, i);
                }

                if (error == SQLITE_ROW) {
                    result.rows.emplace_back(SQResultRow(result, numColumns));
                    for (int i = 0; i < numColumns; i++) {
                        int colType = sqlite3_column_type(preparedStatement, i);
                        switch (colType) {
                            case SQLITE_INTEGER:
                                result.rows.back()[i] = to_string(sqlite3_column_int64(preparedStatement, i));
                                break;
                            case SQLITE_FLOAT:
                                result.rows.back()[i] = to_string(sqlite3_column_double(preparedStatement, i));
                                break;
                            case SQLITE_TEXT:
                                result.rows.back()[i] = reinterpret_cast<const char*>(sqlite3_column_text(preparedStatement, i));
                                break;
                            case SQLITE_BLOB:
                                result.rows.back()[i] = string(static_cast<const char*>(sqlite3_column_blob(preparedStatement, i)), sqlite3_column_bytes(preparedStatement, i));
                                break;
                            case SQLITE_NULL:
                                // null string.
                                break;
                        }
                    }
                } else {
                    if (error == SQLITE_DONE) {
                        // Treat "done" as just not-an-error.
                        error = SQLITE_OK;
                    }
                    break;
                }
            }
            sqlite3_finalize(preparedStatement);
        } while (*statementRemainder != 0 && error == SQLITE_OK);

        extErr = sqlite3_extended_errcode(db);
        if (error != SQLITE_BUSY || extErr == SQLITE_BUSY_SNAPSHOT) {
            break;
        }
        SWARN("sqlite3 returned SQLITE_BUSY on try #"
              << (tries + 1) << " of " << MAX_TRIES << ". "
              << "Extended error code: " << sqlite3_extended_errcode(db) << ". "
              << (((tries + 1) < MAX_TRIES) ? "Sleeping 1 second and re-trying." : "No more retries."));

        // Avoid the sleep after the last try.
        if ((tries + 1) < MAX_TRIES) {
            sleep(1);
        }
    }

    if (error == SQLITE_CORRUPT) {
        SERROR("Database corruption was detected, cannot continue, bedrock will exit immediately.");
    }

    uint64_t elapsed = STimeNow() - startTime;
    if ((int64_t)elapsed > warnThreshold || (int64_t)elapsed > 10000) {
        // Avoid logging queries so long that we need dozens of lines to log them.
        string sqlToLog = sql.substr(0, 20000);
        SRedactSensitiveValues(sqlToLog);

        if ((int64_t)elapsed > warnThreshold) {
            if (isSyncThread) {
                SWARN("Slow query sync ("
                      << "loops: " << numLoops << ", "
                      << "prepare US: " << prepareTimeUS << ", "
                      << "steps: " << numSteps << ", "
                      << "step US: " << stepTimeUS << ", "
                      << "longest step US: " << longestStepTimeUS << "): "
                      << sqlToLog);
            } else {
                SWARN("Slow query (" << elapsed / 1000 << "ms): " << sqlToLog);
            }
        } else {
            // We log the time the queries took, as long as they are over 10ms (to reduce noise of many queries that are
            // consistently faster)
            SINFO("Query completed (" << elapsed / 1000 << "ms): " << sqlToLog);
        }
    }

    // Log this if enabled
    if (_g_sQueryLogFP) {
        string sqlToLog = sql.substr(0, 20000);

        // Log this query as an SQL statement ready for insertion
        const string& dbFilename = sqlite3_db_filename(db, "main");
        const string& csvRow =
            "\"" + dbFilename + "\", " + "\"" + SEscape(STrim(sqlToLog), "\"", '"') + "\", " + SToStr(elapsed) + "\n";
        SASSERT(fwrite(csvRow.c_str(), 1, csvRow.size(), _g_sQueryLogFP) == csvRow.size());
    }

    // Only OK and commit conflicts are allowed without warning because they're the only "successful" results that we expect here.
    // OK means it succeeds, conflicts will get retried further up the call stack.
    if (error != SQLITE_OK && extErr != SQLITE_BUSY_SNAPSHOT && !skipWarn) {
        string sqlToLog = sql.substr(0, 20000);
        SRedactSensitiveValues(sqlToLog);

        SWARN("'" << e << "', query failed with error #" << error << " (" << sqlite3_errmsg(db) << "): " << sqlToLog);
    }

    // But we log for commit conflicts as well, to keep track of how often this happens with this experimental feature.
    if (extErr == SQLITE_BUSY_SNAPSHOT) {
        SHMMM("[concurrent] commit conflict.");
        return extErr;
    }
    return error;
}

// --------------------------------------------------------------------------
// Creates a table, if not there, or verifies it's defined correctly
bool SQVerifyTable(sqlite3* db, const string& tableName, const string& sql) {
    // First, see if it's there
    SQResult result;
    SASSERT(!SQuery(db, "SQVerifyTable", "SELECT * FROM sqlite_master WHERE tbl_name=" + SQ(tableName), result));
    if (result.empty()) {
        // Table doesn't already exist, create it
        SINFO("Creating '" << tableName << "'");
        SASSERT(!SQuery(db, "SQVerifyTable", sql));
        return true; // Created new table
    } else {
        // Table exists, verify it's correct
        SINFO("'" << tableName << "' already exists, verifying. ");
        SASSERT(result[0][4] == sql);
        return false; // Table already exists with correct definition
    }
}

bool SQVerifyTableExists(sqlite3* db, const string& tableName) {
    SQResult result;
    SASSERT(!SQuery(db, "SQVerifyTable", "SELECT * FROM sqlite_master WHERE tbl_name=" + SQ(tableName), result));
    return !result.empty();
}

string SGetCurrentExceptionName()
{
    // __cxa_demangle takes all its parameters by reference, so we create a buffer where it can demangle the current
    // exception name.
    int status = 0;
    size_t length = 1000;
    char buffer[length];
    memset(buffer, 0, length);

    // Demangle the name of the current exception.
    // See: https://libcxxabi.llvm.org/spec.html for details on this ABI interface.
    abi::__cxa_demangle(abi::__cxa_current_exception_type()->name(), buffer, &length, &status);
    string exceptionName = buffer;

    // If it failed, use the original name instead.
    if (status) {
        exceptionName = "(mangled) "s + abi::__cxa_current_exception_type()->name();
    }
    return exceptionName;
}

void STerminateHandler(void) {
    // Alert.
    SALERT("Terminating with uncaught exception '" << SGetCurrentExceptionName() << "'.");

    // And we're out.
    abort();
}

bool SIsValidSQLiteDateModifier(const string& modifier) {
    // See: https://www.sqlite.org/lang_datefunc.html
    list<string> parts = SParseList(SToUpper(modifier));
    for (const string& part : parts) {
        // Simple regexp validation
        if (SREMatch("^(\\+|-)\\d{1,8} (SECOND)S?$", part)) {
            continue;
        }
        if (SREMatch("^(\\+|-)\\d{1,3} (YEAR|MONTH|DAY|HOUR|MINUTE)S?$", part)) {
            continue;
        }
        if (SREMatch("^START OF (DAY|MONTH|YEAR)$", part)) {
            continue;
        }
        if (SREMatch("^WEEKDAY [0-6]$", part)) {
            continue;
        }

        // Couldn't match this part to any valid syntax
        SINFO("Syntax error, failed parsing date modifier '" << modifier << "' on part '" << part << "'");
        return false;
    }

    // Matched all parts, valid syntax
    return true;
}

bool SREMatch(const string& regExp, const string& s) {
    return pcrecpp::RE(regExp).FullMatch(s);
}

bool SREMatch(const string& regExp, const string& s, string& match) {
    return pcrecpp::RE(regExp).FullMatch(s, &match);
}

void SRedactSensitiveValues(string& s) {
    // This code removing authTokens is a quick fix and should be removed once https://github.com/Expensify/Expensify/issues/144185 is done.
    // The message may be truncated midway through the authToken, so there may not be a closing quote (") at the end of
    // the authToken, so we need to optionally match the closing quote with a question mark (?).
    pcrecpp::RE("\"authToken\":\".*\"?").GlobalReplace("\"authToken\":<REDACTED>", &s);

    // Redact queries that contain encrypted fields since there's no value in logging them.
    pcrecpp::RE("v[0-9]+:[0-9A-F]{10,}").GlobalReplace("<REDACTED>", &s);

    // Remove anything inside "html" because we intentionally don't log chats.
    pcrecpp::RE("\"html\":\".*\"").GlobalReplace("\"html\":\"<REDACTED>\"", &s);
}

SStopwatch::SStopwatch() {
    start();
    alarmDuration.store(0);
}

SStopwatch::SStopwatch(uint64_t alarm) {
    startTime.store(0);
    alarmDuration.store(alarm);
}

uint64_t SStopwatch::elapsed() const {
    return STimeNow() - startTime.load();
}

uint64_t SStopwatch::ringing() const {
    return alarmDuration.load() && (elapsed() > alarmDuration.load());
}

void SStopwatch::start() {
    startTime.store(STimeNow());
}

bool SStopwatch::ding() {
    if (!ringing())
        return false;
    start();
    return true;
}

void SLogLevel(int level) {
    _g_SLogMask = LOG_UPTO(level);
    setlogmask(_g_SLogMask);
}

SAutoThreadPrefix::SAutoThreadPrefix(const SData& request) {
    // Retain the old prefix
    oldPrefix = SThreadLogPrefix;
    const string requestID = request.isSet("requestID") ? request["requestID"] : "xxxxxx";
    SLogSetThreadPrefix(requestID + (request.isSet("logParam") ? " " + request["logParam"] : "") + " ");
}

SAutoThreadPrefix::SAutoThreadPrefix(const string& rID) {
    oldPrefix = SThreadLogPrefix;
    const string requestID = rID.empty() ? "xxxxxx" : rID;
    SLogSetThreadPrefix(requestID + " ");
}

SAutoThreadPrefix::~SAutoThreadPrefix() {
    SLogSetThreadPrefix(oldPrefix);
}

float SToFloat(const string& val) {
    return (float)atof(val.c_str());
}

int SToInt(const string& val) {
    return atoi(val.c_str());
}

int64_t SToInt64(const string& val) {
    return atoll(val.c_str());
}

uint64_t SToUInt64(const string& val) {
    return strtoull(val.c_str(), NULL, 10);
}

bool SContains(const list<string>& valueList, const char* value) {
    return ::find(valueList.begin(), valueList.end(), string(value)) != valueList.end();
}

bool SContains(const string& haystack, const string& needle) {
    return haystack.find(needle) != string::npos;
}

bool SContains(const string& haystack, char needle) {
    return haystack.find(needle) != string::npos;
}

bool SContains(const STable& nameValueMap, const string& name) {
    return (nameValueMap.find(name) != nameValueMap.end());
}

bool SIEquals(const string& lhs, const string& rhs) {
    return !strcasecmp(lhs.c_str(), rhs.c_str());
}

bool SEndsWith(const string& haystack, const string& needle) {
    if (needle.size() > haystack.size())
        return false;
    else
        return (haystack.substr(haystack.size() - needle.size()) == needle);
}

string SStripAllBut(const string& lhs, const string& chars) {
    return SStrip(lhs, chars, true);
}

string SStripNonNum(const string& lhs) {
    return SStripAllBut(lhs, "0123456789");
}

string SEscape(const string& lhs, const string& unsafe, char escaper) {
    return SEscape(lhs.c_str(), unsafe, escaper);
}

string SUnescape(const string& lhs, char escaper) {
    return SUnescape(lhs.c_str(), escaper);
}

string SStripTrim(const string& lhs) {
    return STrim(SStrip(lhs));
}

string SBefore(const string& value, const string& needle) {
    size_t pos = value.find(needle);
    if (pos == string::npos)
        return "";
    else
        return value.substr(0, pos);
}

string SAfter(const string& value, const string& needle) {
    size_t pos = value.find(needle);
    if (pos == string::npos)
        return "";
    else
        return value.substr(pos + needle.size());
}

string SAfterLastOf(const string& value, const string& needle) {
    size_t pos = value.find_last_of(needle);
    if (pos == string::npos)
        return "";
    else
        return value.substr(pos + 1);
}

string SAfterUpTo(const string& value, const string& after, const string& upTo) {
    return (SBefore(SAfter(value, after), upTo));
}

void SAppend(string& lhs, const void* rhs, int num) {
    size_t oldSize = lhs.size();
    lhs.resize(oldSize + num);
    memcpy(&lhs[oldSize], rhs, num);
}

void SAppend(string& lhs, const string& rhs) {
    lhs += rhs;
}

int SParseHTTP(const string& buffer, string& methodLine, STable& nameValueMap, string& content) {
    return SParseHTTP(buffer.c_str(), (int)buffer.size(), methodLine, nameValueMap, content);
}

string SComposeHTTP(const string& methodLine, const STable& nameValueMap, const string& content) {
    string buffer;
    SComposeHTTP(buffer, methodLine, nameValueMap, content);
    return buffer;
}

string SComposeHost(const string& host, int port) {
    return (host + ":" + SToStr(port));
}

bool SHostIsValid(const string& host) {
    string domain;
    uint16_t port = 0;
    return SParseHost(host, domain, port);
}

string SGetDomain(const string& host) {
    string domain;
    uint16_t ignore;
    if (SParseHost(host, domain, ignore))
        return domain;
    else
        return host;
}

string SDecodeURIComponent(const string& value) {
    return SDecodeURIComponent(value.c_str(), (int)value.size());
}

bool SParseList(const string& value, list<string>& valueList, char separator) {
    return SParseList(value.c_str(), valueList, separator);
}

list<string> SParseList(const string& value, char separator) {
    list<string> valueList;
    SParseList(value, valueList, separator);
    return valueList;
}

string SGetJSONArrayFront(const string& jsonArray) {
    list<string> l = SParseJSONArray(jsonArray);
    return l.empty() ? "" : l.front();
};

string SToStr(const sockaddr_in& addr) {
    return SToStr(inet_ntoa(addr.sin_addr)) + ":" + SToStr(ntohs(addr.sin_port));
}

ostream& operator<<(ostream& os, const sockaddr_in& addr) {
    return os << SToStr(addr);
}

string SQ(const char* val) {
    return "'" + SEscape(val, "'", '\'') + "'";
}

string SQ(const string& val) {
    return SQ(val.c_str());
}

string SQ(int val) {
    return SToStr(val);
}

string SQ(unsigned val) {
    return SToStr(val);
}

string SQ(uint64_t val) {
    return SToStr(val);
}

string SQ(int64_t val) {
    return SToStr(val);
}

string SQ(double val) {
    return SToStr(val);
}

int SQuery(sqlite3* db, const char* e, const string& sql, int64_t warnThreshold, bool skipWarn) {
    SQResult ignore;
    return SQuery(db, e, sql, ignore, warnThreshold, skipWarn);
}

string SUNQUOTED_TIMESTAMP(uint64_t when) {
    return SComposeTime("%Y-%m-%d %H:%M:%S", when);
}

string STIMESTAMP(uint64_t when) {
    return SQ(SUNQUOTED_TIMESTAMP(when));
}

string SUNQUOTED_CURRENT_TIMESTAMP() {
    return SUNQUOTED_TIMESTAMP(STimeNow());
}

string SCURRENT_TIMESTAMP() {
    return STIMESTAMP(STimeNow());
}

bool STableComp::operator()(const string& s1, const string& s2) const {
    return lexicographical_compare(s1.begin(), s1.end(), s2.begin(), s2.end(), nocase_compare());
}

bool STableComp::nocase_compare::operator()(const unsigned char& c1, const unsigned char& c2) const {
    return tolower(c1) < tolower(c2);
}

SString::SString() {
}

SString& SString::operator=(const char& from) {
    string::operator=(from);
    return *this;
}

SString& SString::operator=(const unsigned char& from) {
    string::operator=(from);
    return *this;
}

SString& SString::operator=(const bool from) {
    string::operator=(from ? "true" : "false");
    return *this;
}

