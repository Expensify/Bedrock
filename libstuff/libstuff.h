#ifndef LIBSTUFF_H
#define LIBSTUFF_H

#include <poll.h>
#include <libgen.h>
#include <syslog.h>

#include <atomic>
#include <functional>
#include <iomanip>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

// Forward declarations of types only used by reference.
struct sockaddr_in;
struct pollfd;
struct sqlite3;
class SQResult;
class SFastBuffer;
struct SData;

using namespace std;

// Global indicating whether we're running the server on dev or production.
extern atomic<bool> GLOBAL_IS_LIVE;

extern void* SSIGNAL_NOTIFY_INTERRUPT;

// Initialize libstuff on every thread before calling any of its functions
void SInitialize(string threadName = "", const char* processName = 0);

// This function sets a lambda that will be executed while the process is being killed for any reason
// (e.g. it crashed). Since we usually add logs in the lambda function, we'll also need to return the log as a
// string so we can write that log in the crash file. We do that to guaurantee we'll have the log message
// instantly available in the crash file instead of depending on rsyslog, which can be late.
void SSetSignalHandlerDieFunc(function<string()>&& func);

// --------------------------------------------------------------------------
// Assertion stuff
// --------------------------------------------------------------------------
// Test invariants and warns on failure
#define SASSERT(_LHS_)                                                                                                 \
    do {                                                                                                               \
        if (!(_LHS_)) {                                                                                                \
            SERROR("Assertion failed: (" << #_LHS_ << ") != true");                                                    \
        }                                                                                                              \
    } while (false)
#define SASSERTEQUALS(_LHS_, _RHS_)                                                                                    \
    do {                                                                                                               \
        if ((_LHS_) != (_RHS_)) {                                                                                      \
            SERROR("Assertion failed: (" << #_LHS_ << ") != (" << #_RHS_ << "): (" << _LHS_ << ") != (" << _RHS_       \
                                         << ")");                                                                      \
        }                                                                                                              \
    } while (false)
#define SASSERTWARN(_LHS_)                                                                                             \
    do {                                                                                                               \
        if (!(_LHS_)) {                                                                                                \
            SWARN("Assertion failed: (" << #_LHS_ << ") != true");                                                     \
        }                                                                                                              \
    } while (false)
#define SASSERTWARNEQUALS(_LHS_, _RHS_)                                                                                \
    do {                                                                                                               \
        if ((_LHS_) != (_RHS_)) {                                                                                      \
            SWARN("Assertion failed: (" << #_LHS_ << ") != (" << #_RHS_ << "): (" << _LHS_ << ") != (" << _RHS_        \
                                        << ")");                                                                       \
        }                                                                                                              \
    } while (false)

// --------------------------------------------------------------------------
// A very simple name/value pair table with case-insensitive name matching
// --------------------------------------------------------------------------
// See: http://stackoverflow.com/questions/1801892/making-mapfind-operation-case-insensitive
class STableComp {
  public:
    struct nocase_compare {
      bool operator() (const unsigned char& c1, const unsigned char& c2) const;
    };
    bool operator() (const string& s1, const string& s2) const;
};

// An SString is just a string with special assignment operators so that we get automatic conversion from arithmetic
// types.
class SString : public string {
  public:
    // Templated assignment operator for arithmetic types.
    template <typename T>
    typename enable_if<is_arithmetic<T>::value, SString&>::type operator=(const T& from) {
        string::operator=(to_string(from));
        return *this;
    }

    template <typename T>
    SString(const T& from) : string(from) {}
    SString();

    // Templated assignment operator for non-arithmetic types.
    template <typename T>
    typename enable_if<!is_arithmetic<T>::value, SString&>::type operator=(const T& from) {
        string::operator=(from);
        return *this;
    }

    // Chars are special, we don't treat them as integral types, even though they'd normally count.
    SString& operator=(const char& from);

    // The above is also true for unsigned chars.
    SString& operator=(const unsigned char& from);

    // Booleans get converted to strings.
    SString& operator=(const bool from);
};

typedef map<string, SString, STableComp> STable;

// An SException is an exception class that can represent an HTTP-like response, with a method line, headers, and a
// body. The STHROW and STHROW_STACK macros will create an SException that logs it's file, line of creation, and
// (for DEBUG) a stack trace at the same time. They can take, 1, 2, or all 3 of the components of an HTTP response as arguments.
#define STHROW(...)                                           \
do {                                                          \
    SLogStackTrace(LOG_DEBUG);                                \
    throw SException(__FILE__, __LINE__, false, __VA_ARGS__); \
} while (false)

#define STHROW_STACK(...) throw SException(__FILE__, __LINE__, true, __VA_ARGS__)
class SException : public exception {
  private:
    static const int CALLSTACK_LIMIT = 100;
    const string _file;
    const int _line;
    void* _callstack[CALLSTACK_LIMIT];
    int _depth = 0;

  public:
    SException(const string& file = "unknown",
               int line = 0,
               bool generateCallstack = false,
               const string& _method = "",
               const STable& _headers = {},
               const string& _body = "");
    const char* what() const noexcept;
    vector<string> details() const noexcept;

    const string method;
    const STable headers;
    const string body;
};

// Utility function for generating pretty callstacks.
vector<string> SGetCallstack(int depth = 0, void* const* callstack = nullptr) noexcept;

// --------------------------------------------------------------------------
// Time stuff TODO: Replace with std::chrono
// --------------------------------------------------------------------------
#define STIME_US_PER_MS ((uint64_t)1000)
#define STIME_US_PER_S ((uint64_t)1000 * STIME_US_PER_MS)
#define STIME_US_PER_M ((uint64_t)60 * STIME_US_PER_S)
#define STIME_US_PER_H ((uint64_t)60 * STIME_US_PER_M)
#define STIME_US_PER_D ((uint64_t)24 * STIME_US_PER_H)
#define STIME_HZ(_HZ_) (STIME_US_PER_S / (_HZ_))

// Various helper time functions
uint64_t STimeNow();
uint64_t STimeThisMorning(); // Timestamp for this morning at midnight GMT
int SDaysInMonth(int year, int month);
string SComposeTime(const string& format, uint64_t when);
uint64_t STimestampToEpoch(const string& format, const string& timestamp);
timeval SToTimeval(uint64_t when);
string SFirstOfMonth(const string& timeStamp, const int64_t& offset = 0);

// Helpful class for timing
struct SStopwatch {
    // Attributes
    atomic<uint64_t> startTime;
    atomic<uint64_t> alarmDuration;

    // Constructors -- If constructed with an alarm, starts out in the
    // ringing state.  If constructed without an alarm, starts out timing
    // from construction.
    SStopwatch();
    SStopwatch(uint64_t alarm);

    // Accessors
    uint64_t elapsed() const;
    uint64_t ringing() const;

    // Mutators
    void start();
    bool ding();
};

// --------------------------------------------------------------------------
// Signal stuff
// --------------------------------------------------------------------------
// Initializes the signal handling for this thread in particular. The first call to this function will initialize
// the general-purpose signal handling thread.
void SInitializeSignals();

// Returns true if the given signal has been raised. Clears the value of the given signal.
bool SGetSignal(int signum);

// Checks whether the given signal has been raised without clearing it.
bool SCheckSignal(int signum);

// Return the current set of signals.
uint64_t SGetSignals();

// Get a descriptive string for all the currently raised signals.
string SGetSignalDescription();

// Clear all outstanding signals.
void SClearSignals();

void SStopSignalThread();

// And also exception stuff.
string SGetCurrentExceptionName();
void STerminateHandler(void);

// --------------------------------------------------------------------------
// Log stuff
// --------------------------------------------------------------------------
// Log level management
extern atomic<int> _g_SLogMask;
void SLogLevel(int level);

// Stack trace logging
void SLogStackTrace(int level = LOG_WARNING);

// This method will allow plugins to whitelist log params they need to log.
void SWhitelistLogParams(set<string> params);

// This is a drop-in replacement for syslog that directly logs to `/run/systemd/journal/syslog` bypassing journald.
void SSyslogSocketDirect(int priority, const char* format, ...);

// Atomic pointer to the syslog function that we'll actually use. Easy to change to `syslog` or `SSyslogSocketDirect`.
extern atomic<void (*)(int priority, const char *format, ...)> SSyslogFunc;

string addLogParams(string&& message, const STable& params = {});

// **NOTE: rsyslog default max line size is 8k bytes. We split on 7k byte boundaries in order to fit the syslog line prefix and the expanded \r\n to #015#012
#define SWHEREAMI SThreadLogPrefix + "(" + basename((char*)__FILE__) + ":" + SToStr(__LINE__) + ") " + __FUNCTION__ + " [" + SThreadLogName + "] "
#define SSYSLOG(_PRI_, _MSG_, ...)                                              \
    do {                                                                        \
        if (_g_SLogMask & (1 << (_PRI_))) {                                     \
            ostringstream __out;                                                \
            __out << _MSG_;                                                     \
            const string s = addLogParams(__out.str(), ##__VA_ARGS__);          \
            const string prefix = SWHEREAMI;                                    \
            for (size_t i = 0; i < s.size(); i += 7168) {                       \
                (*SSyslogFunc)(_PRI_, "%s", (prefix + s.substr(i, 7168)).c_str()); \
            }                                                                   \
        }                                                                       \
    } while (false)

#define SLOGPREFIX ""
#define SDEBUG(_MSG_, ...) SSYSLOG(LOG_DEBUG, "[dbug] " << SLOGPREFIX << _MSG_, ##__VA_ARGS__)
#define SINFO(_MSG_, ...) SSYSLOG(LOG_INFO, "[info] " << SLOGPREFIX << _MSG_, ##__VA_ARGS__)
#define SHMMM(_MSG_, ...) SSYSLOG(LOG_NOTICE, "[hmmm] " << SLOGPREFIX << _MSG_, ##__VA_ARGS__)
#define SWARN(_MSG_, ...) SSYSLOG(LOG_WARNING, "[warn] " << SLOGPREFIX << _MSG_, ##__VA_ARGS__)
#define SALERT(_MSG_, ...) SSYSLOG(LOG_ALERT, "[alrt] " << SLOGPREFIX << _MSG_, ##__VA_ARGS__)
#define SERROR(_MSG_, ...)                                  \
    do {                                                    \
        SSYSLOG(LOG_ERR, "[eror] " << SLOGPREFIX << _MSG_, ##__VA_ARGS__); \
        SLogStackTrace();                                   \
        abort();                                            \
    } while (false)

// --------------------------------------------------------------------------
// Thread stuff
// --------------------------------------------------------------------------

// Each thread gets its own thread-local log prefix.
extern thread_local string SThreadLogPrefix;
extern thread_local string SThreadLogName;

extern thread_local bool isSyncThread;

// Thread-local log prefix
void SLogSetThreadPrefix(const string& logPrefix);
void SLogSetThreadName(const string& name);

struct SAutoThreadPrefix {
    // Set on construction; reset on destruction
    SAutoThreadPrefix(const SData& request);
    SAutoThreadPrefix(const string& rID);
    ~SAutoThreadPrefix();

  private:
    // Attributes
    string oldPrefix;
};
#define SAUTOPREFIX(_PREFIX_) SAutoThreadPrefix __SAUTOPREFIX##__LINE__(_PREFIX_)

// Automatically locks/unlocks a mutex by scope
#define SAUTOLOCK(_MUTEX_) lock_guard<decltype(_MUTEX_)> __SAUTOLOCK_##__LINE__(_MUTEX_);

// Template specialization for atomic strings.
// As the standard library doesn't provide its own template specialization for atomic strings, we provide one here so
// that strings can be used in an atomic fashion in the same way the integral types and trivially-copyable classes are,
// with the same interface. Note that this is not a lock-free implementation, and thus may suffer worse performance
// than many of the standard library specializations.
namespace std {
    template<>
    struct atomic<string> {
        string operator=(const string& desired) {
            lock_guard<decltype(m)> l(m);
            _string = desired;
            return _string;
        }
        bool is_lock_free() const {
            return false;
        }
        void store(const string& desired, [[maybe_unused]] std::memory_order order = std::memory_order_seq_cst) {
            lock_guard<decltype(m)> l(m);
            _string = desired;
        };
        string load([[maybe_unused]] std::memory_order order = std::memory_order_seq_cst) const {
            lock_guard<decltype(m)> l(m);
            return _string;
        }
        operator string() const {
            lock_guard<decltype(m)> l(m);
            return _string;
        }
        string exchange(const string& desired, [[maybe_unused]] std::memory_order order = std::memory_order_seq_cst) {
            lock_guard<decltype(m)> l(m);
            string existing = _string;
            _string = desired;
            return existing;
        };

      private:
        string _string;
        mutable recursive_mutex m;
    };
};

// --------------------------------------------------------------------------
// Math stuff
// --------------------------------------------------------------------------
// Converting between various bases
string SToHex(uint64_t value, int digits = 16);
string SToHex(uint32_t value);
string SToHex(const string& buffer);
uint64_t SFromHex(const string& value);
string SStrFromHex(const string& buffer);
string SBase32HexStringFromBase32(const string& buffer);
string SHexStringFromBase32(const string& buffer);

// Testing various conditions
#define SWITHIN(_MIN_, _VAL_, _MAX_) (((_MIN_) <= (_VAL_)) && ((_VAL_) <= (_MAX_)))

// --------------------------------------------------------------------------
// String stuff
// --------------------------------------------------------------------------
// General utility to convert non-string input to string output
// **NOTE: Use 'ostringstream' because 'stringstream' leaks on VS2005
template <class T> inline string SToStr(const T& t) {
    ostringstream ss;
    ss << fixed << showpoint << setprecision(6) << t;
    return ss.str();
}

// Numeric conversion
float SToFloat(const string& val);
int SToInt(const string& val);
int64_t SToInt64(const string& val);
uint64_t SToUInt64(const string& val);

// General utility for testing map containment
template <class A, class B, class C> inline bool SContains(const map<A, B, C>& nameValueMap, const A& name) {
    return (nameValueMap.find(name) != nameValueMap.end());
}
template <class A> inline bool SContains(const list<A>& valueList, const A& value) {
    return ::find(valueList.begin(), valueList.end(), value) != valueList.end();
}
template <class A> inline bool SContains(const set<A>& valueList, const A& value) {
    return valueList.find(value) != valueList.end();
}

bool SContains(const list<string>& valueList, const char* value);
bool SContains(const string& haystack, const string& needle);
bool SContains(const string& haystack, char needle);
bool SContains(const STable& nameValueMap, const string& name);

bool SIsValidSQLiteDateModifier(const string& modifier);

// General testing functions
bool SIEquals(const string& lhs, const string& rhs);
bool SIContains(const string& haystack, const string& needle);
bool SStartsWith(const string& haystack, const string& needle);
bool SStartsWith(const char* haystack, size_t haystackSize, const char* needle, size_t needleSize);
bool SEndsWith(const string& haystack, const string& needle);
bool SConstantTimeEquals(const string& secret, const string& userInput);
bool SConstantTimeIEquals(const string& secret, const string& userInput);

// Unless `partialMatch` is specified, perform a full regex match (the '^' and '$' symbols are implicit).
//
// If `matches` is supplied it will be cleared, and any matches to the expression will fill it. The first entry in matches will be the entire matched portion of the string,
// and any following entries will be matched parenthesized subgroups.
//
// startOffset can be supplied to ignore the first part of the input string.
//
// If matchOffset is supplied, and a match is found, it will be set to the offset of the first character of the matched substring.
// To find the end of the matched substring, you can do something like matchOffset + matches[0].size().
bool SREMatch(const string& regExp, const string& input, bool caseSensitive = true, bool partialMatch = false, vector<string>* matches = nullptr, size_t startOffset = 0, size_t* matchOffset = nullptr);

// Matches every instance of regExp in the input string. Returns a vector of vectors or strings.
// The outer vector has one entry for each match found. The inner vectors contain first the entire matched substring, and following that, each match group
vector<vector <string>> SREMatchAll(const string& regExp, const string& input, bool caseSensitive = true);

// Replaces all instances of the matched `regExp` with `replacement` in `input`.
string SREReplace(const string& regExp, const string& input, const string& replacement, bool caseSensitive = true);

// Redact values that should not be logged.
void SRedactSensitiveValues(string& s);

// Case testing and conversion
string SToLower(string value);
string SToUpper(string value);

// String alteration
string SCollapse(const string& lhs);
string STrim(const string& lhs);
string SStrip(const string& lhs);
string SStrip(const string& lhs, const string& chars, bool charsAreSafe);
string SStripAllBut(const string& lhs, const string& chars);
string SStripNonNum(const string& lhs);
string SEscape(const char* lhs, const string& unsafe, char escaper);
string SEscape(const string& lhs, const string& unsafe, char escaper = '\\');
string SUnescape(const char* lhs, char escaper);
string SUnescape(const string& lhs, char escaper = '\\');
string SStripTrim(const string& lhs);
string SBefore(const string& value, const string& needle);
string SAfter(const string& value, const string& needle);
string SAfterLastOf(const string& value, const string& needle);
string SAfterUpTo(const string& value, const string& after, const string& upTo);
string SReplace(const string& value, const string& find, const string& replace);
string SReplaceAllBut(const string& value, const string& safeChars, char replaceChar);
string SReplaceAll(const string& value, const string& unsafeChars, char replaceChar);
int SStateNameToInt(const char* states[], const string& stateName, unsigned int numStates);
void SAppend(string& lhs, const void* rhs, int num);
void SAppend(string& lhs, const string& rhs);

// HTTP message management
int SParseHTTP(const char* buffer, size_t length, string& methodLine, STable& nameValueMap, string& content);
int SParseHTTP(const string& buffer, string& methodLine, STable& nameValueMap, string& content);
bool SParseRequestMethodLine(const string& methodLine, string& method, string& uri);
bool SParseResponseMethodLine(const string& methodLine, string& protocol, int& code, string& reason);
bool SParseURI(const char* buffer, int length, string& host, string& path);
bool SParseURI(const string& uri, string& host, string& path);
bool SParseURIPath(const char* buffer, int length, string& path, STable& nameValueMap);
bool SParseURIPath(const string& uri, string& path, STable& nameValueMap);
void SComposeHTTP(string& buffer, const string& methodLine, const STable& nameValueMap, const string& content);
string SComposeHTTP(const string& methodLine, const STable& nameValueMap, const string& content);
string SComposePOST(const STable& nameValueMap);
string SComposeHost(const string& host, int port);
bool SParseHost(const string& host, string& domain, uint16_t& port);
bool SHostIsValid(const string& host);
string SGetDomain(const string& host);
string SDecodeURIComponent(const char* buffer, int length);
string SDecodeURIComponent(const string& value);
string SEncodeURIComponent(const string& value);

// --------------------------------------------------------------------------
// List stuff
// --------------------------------------------------------------------------
// List management
list<int64_t> SParseIntegerList(const string& value, char separator = ',');
set<int64_t> SParseIntegerSet(const string& value, char separator = ',');
vector<int64_t> SParseIntegerVector(const string& value, char separator = ',');
bool SParseList(const char* value, list<string>& valueList, char separator = ',');
bool SParseList(const string& value, list<string>& valueList, char separator = ',');
set<string> SParseSet(const string& value, char separator = ',');
list<string> SParseList(const string& value, char separator = ',');

// Concatenates things into a string. "Things" can mean essentially any
// standard STL container of any type of object that "stringstream" can handle.
template <typename T> string SComposeList(const T& valueList, const string& separator = ", ") {
    if (valueList.empty()) {
        return "";
    }
    string working;
    for(auto value : valueList) {
        working += SToStr(value);
        working += separator;
    }
    return working.substr(0, working.size() - separator.size());
}

// --------------------------------------------------------------------------
// JSON stuff
// --------------------------------------------------------------------------
// JSON message management
string SToJSON(const string& value, const bool forceString = false);
string SToJSON(const int64_t value, const bool forceString = false);

template <typename T>
string SComposeJSONArray(const T& valueList) {
    if (valueList.empty()) {
        return "[]";
    }
    string working = "[";
    for (auto value : valueList) {
        working += SToJSON(value) + ",";
    }
    working.resize(working.size() - 1);
    working += "]";
    return working;
}

string SComposeJSONObject(const STable& nameValueMap, const bool forceString = false);
STable SParseJSONObject(const string& object);
list<string> SParseJSONArray(const string& array);
string SGetJSONArrayFront(const string& jsonArray);

// --------------------------------------------------------------------------
// Network stuff
// --------------------------------------------------------------------------

// Converts a sockaddr_in to a string of the form "aaa.bbb.ccc.ddd:port"
string SToStr(const sockaddr_in& addr);
ostream& operator<<(ostream& os, const sockaddr_in& addr);

// map of FDs to pollfds
typedef map<int, pollfd> fd_map;
#define SREADEVTS (POLLIN | POLLPRI | POLLHUP)
#define SWRITEEVTS (POLLOUT)

// This will add the events specified in `evts` to the events we'll listen for for this socket,
// or, if this socket isn't in our set, it'll add it.
void SFDset(fd_map& fdm, int socket, short evts);

// Returns true if *ANY* of the bits in evts are set as returned value for this socket.
// Returns false otherwise, or if this socket isn't in this fd_set, or if evts is 0.
bool SFDAnySet(fd_map& fdm, int socket, short evts);

// Socket helpers
int S_socket(const string& host, bool isTCP, bool isPort, bool isBlocking);
int S_accept(int port, sockaddr_in& fromAddr, bool isBlocking);
ssize_t S_recvfrom(int s, char* recvBuffer, int recvBufferSize, sockaddr_in& fromAddr);
bool S_recvappend(int s, SFastBuffer& recvBuffer);
bool S_sendconsume(int s, SFastBuffer& sendBuffer);
int S_poll(fd_map& fdm, uint64_t timeout);

// Network helpers
string SGetHostName();
string SGetPeerName(int s);

// Common error checking/logging.
bool SCheckNetworkErrorType(const string& logPrefix, const string& peer, int errornumber);

// --------------------------------------------------------------------------
// File stuff
// --------------------------------------------------------------------------
// Basic file loading and saving
bool SFileExists(const string& path);
bool SFileLoad(const string& path, string& buffer);
string SFileLoad(const string& path);
bool SFileSave(const string& path, const string& buffer);
bool SFileDelete(const string& path);
bool SFileCopy(const string& fromPath, const string& toPath);
uint64_t SFileSize(const string& path);

// --------------------------------------------------------------------------
// Crypto stuff
// --------------------------------------------------------------------------
// Various hashing functions
string SHashSHA1(const string& buffer);
string SHashSHA256(const string& buffer);

// Various encoding/decoding functions
string SEncodeBase64(const unsigned char* buffer, const size_t size);
string SEncodeBase64(const string& buffer);
string SDecodeBase64(const unsigned char* buffer, const size_t size);
string SDecodeBase64(const string& buffer);

// HMAC (for use with Amazon S3)
string SHMACSHA1(const string& key, const string& buffer);
string SHMACSHA256(const string& key, const string& buffer);

// Encryption/Decryption
#define SAES_KEY_SIZE 32 // AES256 32 bytes = 256 bits
#define SAES_IV_SIZE 16
#define SAES_BLOCK_SIZE 16
string SAESEncrypt(const string& buffer, const string& ivStr, const string& key);
string SAESDecrypt(const string& buffer, unsigned char* iv, const string& key);
string SAESDecrypt(const string& buffer, const string& iv, const string& key);
string SAESDecryptNoStrip(const string& buffer, const size_t& bufferSize, unsigned char* iv, const string& key);
string SAESDecryptNoStrip(const string& buffer, const size_t& bufferSize, const string& iv, const string& key);

// --------------------------------------------------------------------------
// SQLite Stuff
// --------------------------------------------------------------------------
string SQ(const char* val);
string SQ(const string& val);
string SQ(int val);
string SQ(unsigned val);
string SQ(uint64_t val);
string SQ(int64_t val);
string SQ(double val);
string SQList(const string& val, bool integersOnly = true);

template <typename Container> string SQList(const Container& valueList) {
    list<string> safeValues;
    for (typename Container::const_iterator valueIt = valueList.begin(); valueIt != valueList.end(); ++valueIt) {
        safeValues.push_back(SQ(*valueIt));
    }
    return SComposeList(safeValues);
}

void SQueryLogOpen(const string& logFilename);
void SQueryLogClose();

// Returns an SQLite result code.
int SQuery(sqlite3* db, const char* e, const string& sql, SQResult& result, int64_t warnThreshold = 2000 * STIME_US_PER_MS, bool skipInfoWarn = false);
int SQuery(sqlite3* db, const char* e, const string& sql, int64_t warnThreshold = 2000 * STIME_US_PER_MS, bool skipInfoWarn = false);
bool SQVerifyTable(sqlite3* db, const string& tableName, const string& sql);
bool SQVerifyTableExists(sqlite3* db, const string& tableName);

// --------------------------------------------------------------------------
string SUNQUOTED_TIMESTAMP(uint64_t when);
string STIMESTAMP(uint64_t when);
string SUNQUOTED_CURRENT_TIMESTAMP();
string SCURRENT_TIMESTAMP();
string SCURRENT_TIMESTAMP_MS();
string STIMESTAMP_MS(uint64_t time);

// --------------------------------------------------------------------------
// Miscellaneous stuff
// --------------------------------------------------------------------------
// Compression
string SGZip(const string& content);
string SGUnzip(const string& content);

// Command-line helpers
STable SParseCommandLine(int argc, char* argv[]);

// Returns the CPU usage inside the current thread
double SGetCPUUserTime();

#endif	// LIBSTUFF_H
