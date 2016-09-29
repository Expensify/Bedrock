// --------------------------------------------------------------------------
// libstuff.h
// --------------------------------------------------------------------------
#pragma once

// Include relevant headers
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <syslog.h>
#include <unistd.h>
#include <fcntl.h>
#include <pcrecpp.h> // sudo apt-get install libpcre++-dev
#include <poll.h>
#include <time.h>
#include <libgen.h>   // for basename()
#include <sys/time.h> // for gettimeofday()
#include <signal.h>
#include <pthread.h>

// Define places where we must follow Win32's lead
#define closesocket(_S_) close(_S_)
#define mkgmtime timegm

// --------------------------------------------------------------------------
// Initialization / Shutdown
// --------------------------------------------------------------------------
// Initialize libstuff on every thread before calling any of its functions
extern void SInitialize();

// Runs an internal test
extern int STestLibStuff();

// --------------------------------------------------------------------------
// Standard Template Library stuff
// --------------------------------------------------------------------------
// Include the files
#include <map>
#include <string>
#include <list>
#include <iostream>
#include <sstream>
#include <vector>
#include <set>
#include <algorithm>
#include <stdlib.h>
using namespace std;

// Useful STL macros
// _CT_ : Container type
// _C_  : Container
// _I_  : Iterator
#define SFOREACH(_CT_, _C_, _I_) for (_CT_::iterator _I_ = (_C_).begin(); _I_ != (_C_).end(); ++_I_)
#define SFOREACHREVERSE(_CT_, _C_, _I_) for (_CT_::reverse_iterator _I_ = (_C_).rbegin(); _I_ != (_C_).rend(); ++_I_)
#define SFOREACHCONST(_CT_, _C_, _I_) for (_CT_::const_iterator _I_ = (_C_).begin(); _I_ != (_C_).end(); ++_I_)
#define SFOREACHMAP(_CT0_, _CT1_, _C_, _I_)                                                                            \
    for (map<_CT0_, _CT1_>::iterator _I_ = (_C_).begin(); _I_ != (_C_).end(); ++_I_)
#define SFOREACHMAPREVERSE(_CT0_, _CT1_, _C_, _I_)                                                                     \
    for (map<_CT0_, _CT1_>::reverse_iterator _I_ = (_C_).rbegin(); _I_ != (_C_).rend(); ++_I_)
#define SFOREACHMAPCONST(_CT0_, _CT1_, _C_, _I_)                                                                       \
    for (map<_CT0_, _CT1_>::const_iterator _I_ = (_C_).begin(); _I_ != (_C_).end(); ++_I_)
#define SFOREACHTABLE(_C_, _I_) for (STable::const_iterator _I_ = (_C_).begin(); _I_ != (_C_).end(); ++_I_)

// --------------------------------------------------------------------------
// Assertion stuff
// --------------------------------------------------------------------------
// Test invariants and warns on failure
#define SASSERT(_LHS_)                                                                                                 \
    do {                                                                                                               \
        if (!(_LHS_)) {                                                                                                \
            SERROR("Assertion failed: (" << #_LHS_ << ") != true");                                                    \
            abort();                                                                                                   \
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
#define SASSERTTHROW(condition, uuid)                                                                                  \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            throw AssertionFailedException(#condition, uuid);                                                          \
        }                                                                                                              \
    } while (false)

// --------------------------------------------------------------------------
// A very simple name/value pair table with case-insensitive name maching
// --------------------------------------------------------------------------
// See: http://stackoverflow.com/questions/1801892/making-mapfind-operation-case-insensitive
class STableComp : binary_function<string, string, bool>
{
  public:
    bool operator()(const string& s1, const string& s2) const
    {
        return lexicographical_compare(s1.begin(), s1.end(), s2.begin(), s2.end(), nocase_compare());
    }

  private:
    class nocase_compare : public binary_function<unsigned char, unsigned char, bool>
    {
      public:
        bool operator()(const unsigned char& c1, const unsigned char& c2) const
        {
            return tolower(c1) < tolower(c2);
        }
    };
};
typedef map<string, string, STableComp> STable;

// --------------------------------------------------------------------------
// A very simple HTTP-like structure consisting of a method line, a table,
// and a content body.
// --------------------------------------------------------------------------
struct SData {
    // Public attributes
    string methodLine;
    STable nameValueMap;
    string content;

    // Constructors
    SData();
    SData(const string& method);

    // Operators
    string& operator[](const string& name);
    string operator[](const string& name) const;

    // Mutators
    void clear();
    void erase(const string& name);
    void merge(const STable& rhs);
    void merge(const SData& rhs);

    // Accessors
    bool empty() const;
    bool isSet(const string& name) const;
    int calc(const string& name) const;
    int64_t calc64(const string& name) const;
    uint64_t calcU64(const string& name) const;
    bool test(const string& name) const;
    string getVerb() const;

    // Serialization
    void serialize(ostringstream& out) const;
    string serialize() const;
    int deserialize(const string& rhs);
    int deserialize(const char* buffer, int length);

    // Create an SData object; if no Content-Length then take everything as the content
    static SData create(const string& rhs);
};

// --------------------------------------------------------------------------
// Time stuff
// --------------------------------------------------------------------------
#define STIME_US_PER_MS ((uint64_t)1000)
#define STIME_US_PER_S ((uint64_t)1000 * STIME_US_PER_MS)
#define STIME_US_PER_M ((uint64_t)60 * STIME_US_PER_S)
#define STIME_US_PER_H ((uint64_t)60 * STIME_US_PER_M)
#define STIME_US_PER_D ((uint64_t)24 * STIME_US_PER_H)
#define STIME_HZ(_HZ_) (STIME_US_PER_S / (_HZ_))

// Various helper time functions
extern uint64_t STimeNow();
extern uint64_t STimeThisMorning(); // Timestamp for this morning at midnight GMT
extern int SDaysInMonth(int year, int month);
extern string SComposeTime(const string& format, uint64_t when);
extern timeval SToTimeval(uint64_t when);

// Helpful class for timing
struct SStopwatch {
    // Attributes
    uint64_t startTime;
    uint64_t alarmDuration;

    // Constructors -- If constructed with an alarm, starts out in the
    // ringing state.  If constructed without an alarm, starts out timing
    // from construction.
    SStopwatch()
    {
        start();
        alarmDuration = 0;
    }
    SStopwatch(uint64_t alarm)
    {
        startTime     = 0;
        alarmDuration = alarm;
    }

    // Accessors
    uint64_t elapsed()
    {
        return STimeNow() - startTime;
    }
    uint64_t ringing()
    {
        return alarmDuration && (elapsed() > alarmDuration);
    }

    // Mutators
    void start()
    {
        startTime = STimeNow();
    }
    bool ding()
    {
        if (!ringing())
            return false;
        start();
        return true;
    }
};

// --------------------------------------------------------------------------
// Signal stuff
// --------------------------------------------------------------------------
// Returns whether or not a single signal has been sent
extern bool SCatchSignal(int signum);

// Clears all signals that have been previously sent
extern void SClearSignals();

// Returns the bitmask of which signals have been sent
extern uint64_t SGetSignals();

// Manually "sends" one of the signals
extern void SSendSignal(int signum);

// Returns the name of a signal
extern string SGetSignalName(int signum);

// Returns all signals set in a bitmask
extern string SGetSignalNames(uint64_t sigmask);

// --------------------------------------------------------------------------
// Log stuff
// --------------------------------------------------------------------------
// Log level management
extern int _g_SLogMask;
inline void SLogLevel(int level)
{
    _g_SLogMask = LOG_UPTO(level);
    setlogmask(_g_SLogMask);
}
inline bool SLogLevelIsSet(int level)
{
    return (_g_SLogMask & (1 << level));
}

// Stack trace logging
extern void SLogStackTrace();

// Simply logs a stream to the debugger
// **NOTE: rsyslog max line size is 2048 bytes.  We split on 1500 byte bounderies in order to fit the
//         syslog line prefix and the expanded \r\n to #015#012
// **FIXME: Everything submitted to syslog as WARN; doesn't show otherwise
extern bool _g_SLogToSTDOUT;
#define SSYSLOG(_PRI_, _MSG_)                                                                                          \
    do {                                                                                                               \
        SThreadLocalStorage* tls = SThreadGetLocalStorage();                                                           \
        if (_g_SLogMask & (1 << (_PRI_))) {                                                                            \
            ostringstream __out;                                                                                       \
            __out << _MSG_ << endl;                                                                                    \
            const string& __s = __out.str();                                                                           \
            if (_g_SLogToSTDOUT)                                                                                       \
                printf("%s", __s.c_str());                                                                             \
            else                                                                                                       \
                for (int __i = 0; __i < (int)__s.size(); __i += 1500)                                                  \
                    syslog(LOG_WARNING, "%s", __s.substr(__i, 1500).c_str());                                          \
        }                                                                                                              \
    } while (false)

#define SWHEREAMI                                                                                                      \
    tls->logPrefix << "(" << basename((char*)__FILE__) << ":" << __LINE__ << ") " << __FUNCTION__ << " [" << tls->name \
                   << "] "

#define SLOGPREFIX ""
#define SLOG(_MSG_) SSYSLOG(LOG_DEBUG, SWHEREAMI << SLOGPREFIX << _MSG_)
#define SDEBUG(_MSG_) SSYSLOG(LOG_DEBUG, SWHEREAMI << "[dbug] " << SLOGPREFIX << _MSG_)
#define SINFO(_MSG_) SSYSLOG(LOG_INFO, SWHEREAMI << "[info] " << SLOGPREFIX << _MSG_)
#define SHMMM(_MSG_) SSYSLOG(LOG_WARNING, SWHEREAMI << "[hmmm] " << SLOGPREFIX << _MSG_)
#define SWARN(_MSG_) SSYSLOG(LOG_WARNING, SWHEREAMI << "[warn] " << SLOGPREFIX << _MSG_)
#define SALERT(_MSG_) SSYSLOG(LOG_WARNING, SWHEREAMI << "[alrt] " << SLOGPREFIX << _MSG_)
#define SERROR(_MSG_)                                                                                                  \
    do {                                                                                                               \
        SSYSLOG(LOG_ERR, SWHEREAMI << "[eror] " << SLOGPREFIX << _MSG_);                                               \
        SLogStackTrace();                                                                                              \
        fflush(stdout);                                                                                                \
        exit(1);                                                                                                       \
    } while (false)
#define STRACE() SLOG("[trac] " << __FILE__ << "(" << __LINE__ << ") :" << __FUNCTION__)

// Convenience class for maintaining connections with a mesh of peers
#define PDEBUG(_MSG_) SDEBUG("->{" << peer->name << "} " << _MSG_)
#define PINFO(_MSG_) SINFO("->{" << peer->name << "} " << _MSG_)
#define PHMMM(_MSG_) SHMMM("->{" << peer->name << "} " << _MSG_)
#define PWARN(_MSG_) SWARN("->{" << peer->name << "} " << _MSG_)

// --------------------------------------------------------------------------
// Thread stuff
// --------------------------------------------------------------------------
// Light wrapper around thread functions
void* SThreadOpen(void (*proc)(void* procData), void* procData, const string& threadName = "", size_t stackSize = 0);
extern void SThreadClose(void* thread);
extern void SThreadSleep(uint64_t delay);

// Thread local storage
extern pthread_key_t _g_SThread_TLSKey;
struct SThreadLocalStorage {
    // Attributes
    void (*proc)(void* procData);
    void* procData;
    string name;
    string logPrefix;
    SData data;
};
inline SThreadLocalStorage* SThreadGetLocalStorage()
{
    return (SThreadLocalStorage*)pthread_getspecific(_g_SThread_TLSKey);
}

// Thread-local log prefix
inline void SLogSetThreadPrefix(const string& logPrefix)
{
    SThreadLocalStorage* tls = SThreadGetLocalStorage();
    tls->logPrefix           = logPrefix;
}
struct SAutoThreadPrefix {
    // Set on construction; reset on destruction
    SAutoThreadPrefix(const string& prefix)
    {
        // Retain the old prefix
        oldPrefix = SThreadGetLocalStorage()->logPrefix;

        // Only change if we have something
        if (!prefix.empty()) {
            SLogSetThreadPrefix(prefix + " ");
        }
    }
    ~SAutoThreadPrefix()
    {
        SLogSetThreadPrefix(oldPrefix);
    }

  private:
    // Attributes
    string oldPrefix;
};
#define SAUTOPREFIX(_PREFIX_) SAutoThreadPrefix __SAUTOPREFIX##__LINE__(_PREFIX_)

// Light wrapper around mutex functions
extern void* SMutexOpen();
extern void SMutexLock(void* mutex);
extern void SMutexUnlock(void* mutex);
extern void SMutexClose(void* mutex);

// Automatically locks/unlocks a mutex by scope
#define SAUTOLOCK(_MUTEX_) SAutoMutex __SAUTOLOCK_##__LINE__(_MUTEX_)
struct SAutoMutex {
    // Attributes
    void* mutex;

    // Locks on creation
    SAutoMutex(void* mutex_)
    {
        SASSERT(mutex_);
        mutex = mutex_;
        SMutexLock(mutex);
    }

    // Unlocks on destruction
    ~SAutoMutex()
    {
        SASSERT(mutex);
        SMutexUnlock(mutex);
    }
};

// Convenient interface for multi-threaded, synchronized variables.
template <typename T> class SSynchronized
{
  public:
    // Initialize and wrap with a mutex that is cleaned up in the destructor
    SSynchronized(const T& val)
    {
        _mutex             = SMutexOpen();
        _synchronizedValue = val;
    }
    ~SSynchronized()
    {
        SMutexClose(_mutex);
    }

    // Getter and setter
    T get()
    {
        SAUTOLOCK(_mutex);
        return _synchronizedValue;
    }
    void set(const T& val)
    {
        SAUTOLOCK(_mutex);
        _synchronizedValue = val;
    }

  private:
    // Attributes
    T _synchronizedValue;
    void* _mutex;
};

// --------------------------------------------------------------------------
// Memory stuff
// --------------------------------------------------------------------------
// Macros for ensuring allocated memory is cleaned up right
#define SDELETE(_PTR_)                                                                                                 \
    do {                                                                                                               \
        if (_PTR_) {                                                                                                   \
            delete _PTR_;                                                                                              \
            _PTR_ = 0;                                                                                                 \
        }                                                                                                              \
    } while (false)
#define SZERO(_OBJ_) memset(&_OBJ_, 0, sizeof(_OBJ_))

// --------------------------------------------------------------------------
// Math stuff
// --------------------------------------------------------------------------
// Converting between various bases
extern string SToHex(uint64_t value, int digits = 16);
inline string SToHex(uint32_t value)
{
    return SToHex(value, 8);
}
extern string SToHex(const string& buffer);
extern uint64_t SFromHex(const string& value);
extern string SStrFromHex(const string& buffer);
extern string SToBase26(uint64_t value);
extern string SToBase36(uint64_t value);
extern string SClampSize(const string& in, int digits, char fill);

// Testing various conditions
#define SWITHIN(_MIN_, _VAL_, _MAX_) (((_MIN_) <= (_VAL_)) && ((_VAL_) <= (_MAX_)))

// Clamping
template <class T> inline T SMax(T lhs, T rhs)
{
    return (lhs > rhs ? lhs : rhs);
}
template <class T> inline T SMin(T lhs, T rhs)
{
    return (lhs < rhs ? lhs : rhs);
}
#define SABS(_VAL_) ((_VAL_) < 0 ? -1 * (_VAL_) : (_VAL_))

// Random functions
inline uint64_t SRand15()
{
    return (int64_t)(rand() & 0x7FFF);
}
inline uint64_t SRand64()
{
    return ((SRand15() << 60) | (SRand15() << 45) | (SRand15() << 30) | (SRand15() << 15) | (SRand15() << 0));
}

// Helper function to convert from cents to dollars
inline string SToDecimal(int cents)
{
    // Just render with 2 decimal points
    char buf[32];
    sprintf(buf, "%d.%02d", SABS(cents) / 100, SABS(cents) % 100);
    return string(cents < 0 ? "-" : "") + buf;
}

// --------------------------------------------------------------------------
// String stuff
// --------------------------------------------------------------------------
// General utility to convert non-string input to string output
// **NOTE: Use 'ostringstream' because 'stringstream' leaks on VS2005
template <class T> inline string SToStr(const T& t)
{
    ostringstream ss;
    ss << t;
    return ss.str();
}

// Numeric conversion
inline float SToFloat(const string& val)
{
    return (float)atof(val.c_str());
}
inline int SToInt(const string& val)
{
    return atoi(val.c_str());
}
inline int64_t SToInt64(const string& val)
{
    return atoll(val.c_str());
}
inline uint64_t SToUInt64(const string& val)
{
    return strtoull(val.c_str(), NULL, 10);
}

// General utility for testing map containment
template <class A, class B, class C> inline bool SContains(const map<A, B, C>& nameValueMap, const A& name)
{
    return (nameValueMap.find(name) != nameValueMap.end());
}
template <class A> inline bool SContains(const list<A>& valueList, const A& value)
{
    return ::find(valueList.begin(), valueList.end(), value) != valueList.end();
}
template <class A> inline bool SContains(const set<A>& valueList, const A& value)
{
    return ::find(valueList.begin(), valueList.end(), value) != valueList.end();
}

// General testing functions
inline bool SIEquals(const string& lhs, const string& rhs)
{
    return !strcasecmp(lhs.c_str(), rhs.c_str());
}
inline bool SContains(const string& haystack, const string& needle)
{
    return haystack.find(needle) != string::npos;
}
inline bool SContains(const string& haystack, char needle)
{
    return haystack.find(needle) != string::npos;
}
extern bool SIContains(const string& haystack, const string& needle);
inline bool SStartsWith(const string& haystack, const string& needle)
{
    return haystack.find(needle) == 0;
}
inline bool SEndsWith(const string& haystack, const string& needle)
{
    if (needle.size() > haystack.size())
        return false;
    else
        return (haystack.substr(haystack.size() - needle.size()) == needle);
}
extern bool SConstantTimeEquals(const string& secret, const string& userInput);
extern bool SConstantTimeIEquals(const string& secret, const string& userInput);

// Perform a full regex match. The '^' and '$' symbols are implicit.
inline bool SREMatch(const string& regExp, const string& s)
{
    return pcrecpp::RE(regExp).FullMatch(s);
}
inline bool SREMatch(const string& regExp, const string& s, string& match)
{
    return pcrecpp::RE(regExp).FullMatch(s, &match);
}

// Case testing and conversion
inline char SToLower(char from)
{
    return (SWITHIN('A', from, 'Z') ? from - 'A' + 'a' : from);
}
inline char SToUpper(char from)
{
    return (SWITHIN('a', from, 'z') ? from - 'a' + 'A' : from);
}
inline string SToLower(const string& value)
{
    string out;
    out.resize(value.size());
    for (int c = 0; c < (int)value.size(); ++c)
        out[c] = SToLower(value[c]);
    return out;
}
inline string SToUpper(const string& value)
{
    string out;
    out.resize(value.size());
    for (int c = 0; c < (int)value.size(); ++c)
        out[c] = SToUpper(value[c]);
    return out;
}

// String alteration
extern string SCollapse(const string& lhs);
extern string STrim(const string& lhs);
extern string SStrip(const string& lhs);
extern string SStrip(const string& lhs, const string& chars, bool charsAreSafe);
inline string SStripAllBut(const string& lhs, const string& chars)
{
    return SStrip(lhs, chars, true);
}
inline string SStripNonNum(const string& lhs)
{
    return SStripAllBut(lhs, "0123456789");
}
extern string SEscape(const char* lhs, const string& unsafe, char escaper);
inline string SEscape(const string& lhs, const string& unsafe, char escaper = '\\')
{
    return SEscape(lhs.c_str(), unsafe, escaper);
}
extern string SUnescape(const char* lhs, char escaper);
inline string SUnescape(const string& lhs, char escaper = '\\')
{
    return SUnescape(lhs.c_str(), escaper);
}
inline string SStripTrim(const string& lhs)
{
    return STrim(SStrip(lhs));
}
inline string SBefore(const string& value, const string& needle)
{
    size_t pos = value.find(needle);
    if (pos == string::npos)
        return "";
    else
        return value.substr(0, pos);
}
inline string SAfter(const string& value, const string& needle)
{
    size_t pos = value.find(needle);
    if (pos == string::npos)
        return "";
    else
        return value.substr(pos + needle.size());
}
inline string SAfterLastOf(const string& value, const string& needle)
{
    size_t pos = value.find_last_of(needle);
    if (pos == string::npos)
        return "";
    else
        return value.substr(pos + 1);
}
inline string SBetween(const string& value, const string& lhs, const string& rhs)
{
    return SBefore(SAfter(value, lhs), rhs);
}
inline bool SInsertAfter(string& value, const string& needle, const string& thread)
{
    size_t pos = value.find(needle);
    if (pos == string::npos)
        return false;
    value.insert(pos + needle.size(), thread);
    return true;
}
inline string SAfterUpTo(const string& value, const string& after, const string& upTo)
{
    return (SBefore(SAfter(value, after), upTo));
}
extern string SReplace(const string& value, const string& find, const string& replace);
extern string SReplaceAllBut(const string& value, const string& safeChars, char replaceChar);
extern string SReplaceAll(const string& value, const string& unsafeChars, char replaceChar);
extern int SStateNameToInt(const char* states[], const string& stateName, unsigned int numStates);

// Stream management
extern void SConsumeFront(string& lhs, ssize_t num);
inline void SConsumeBack(string& lhs, int num)
{
    if ((int)lhs.size() <= num) {
        lhs.clear();
    } else {
        lhs = lhs.substr(0, lhs.size() - num);
    }
}
inline void SAppend(string& lhs, const char* rhs, int num)
{
    size_t oldSize = lhs.size();
    lhs.resize(oldSize + num);
    memcpy(&lhs[oldSize], rhs, num);
}

// HTTP message management
#define S_COOKIE_SEPARATOR ((char)0xFF)
extern int SParseHTTP(const char* buffer, size_t length, string& methodLine, STable& nameValueMap, string& content);
inline int SParseHTTP(const string& buffer, string& methodLine, STable& nameValueMap, string& content)
{
    return SParseHTTP(buffer.c_str(), (int)buffer.size(), methodLine, nameValueMap, content);
}
extern bool SParseRequestMethodLine(const string& methodLine, string& method, string& uri);
extern bool SParseResponseMethodLine(const string& methodLine, string& protocol, int& code, string& reason);
extern bool SParseURI(const char* buffer, int length, string& host, string& path);
inline bool SParseURI(const string& uri, string& host, string& path)
{
    return SParseURI(uri.c_str(), (int)uri.size(), host, path);
}
extern bool SParseURIPath(const char* buffer, int length, string& path, STable& nameValueMap);
inline bool SParseURIPath(const string& uri, string& path, STable& nameValueMap)
{
    return SParseURIPath(uri.c_str(), (int)uri.size(), path, nameValueMap);
}
extern void SComposeHTTP(string& buffer, const string& methodLine, const STable& nameValueMap, const string& content);
inline string SComposeHTTP(const string& methodLine, const STable& nameValueMap, const string& content)
{
    string buffer;
    SComposeHTTP(buffer, methodLine, nameValueMap, content);
    return buffer;
}
extern string SComposePOST(const STable& nameValueMap);
inline string SComposeHost(const string& host, int port)
{
    return (host + ":" + SToStr(port));
}
extern bool SParseHost(const string& host, string& domain, uint16_t& port);
inline bool SHostIsValid(const string& host)
{
    string domain;
    uint16_t port = 0;
    return SParseHost(host, domain, port);
}
inline bool SURIIsValid(const string& uri)
{
    string host, path;
    return SParseURI(uri, host, path);
}
inline string SGetDomain(const string& host)
{
    string domain;
    uint16_t ignore;
    if (SParseHost(host, domain, ignore))
        return domain;
    else
        return host;
}
extern string SDecodeURIComponent(const char* buffer, int length);
inline string SDecodeURIComponent(const string& value)
{
    return SDecodeURIComponent(value.c_str(), (int)value.size());
}
extern string SEncodeURIComponent(const string& value);

// --------------------------------------------------------------------------
// List stuff
// --------------------------------------------------------------------------
// List management
extern bool SParseIntegerList(const char* value, list<int64_t>& valueList, char separator = ',');
inline bool SParseIntegerList(const string& value, list<int64_t>& valueList, char separator = ',')
{
    return SParseIntegerList(value.c_str(), valueList, separator);
}
inline list<int64_t> SParseIntegerList(const string& value, char separator = ',')
{
    list<int64_t> valueList;
    SParseIntegerList(value, valueList, separator);
    return valueList;
}

extern bool SParseList(const char* value, list<string>& valueList, char separator = ',');
extern bool SParseVector(const char* value, vector<string>& valueVector, char separator = ',');
inline bool SParseList(const string& value, list<string>& valueList, char separator = ',')
{
    return SParseList(value.c_str(), valueList, separator);
}
inline bool SParseVector(const string& value, vector<string>& valueVector, char separator = ',')
{
    return SParseVector(value.c_str(), valueVector, separator);
}
inline list<string> SParseList(const string& value, char separator = ',')
{
    list<string> valueList;
    SParseList(value, valueList, separator);
    return valueList;
}
inline vector<string> SParseVector(const string& value, char separator = ',')
{
    vector<string> valueVector;
    SParseVector(value, valueVector, separator);
    return valueVector;
}

// Concatenates things into a string. "Things" can mean essentially any
// standard STL container of any type of object that "stringstream" can handle.
template <typename T> string SComposeList(const T& valueList, const string& separator = ", ")
{
    if (valueList.empty()) {
        return "";
    }
    string working;
    for_each(valueList.begin(), valueList.end(), [&](decltype(typename T::value_type()) value) {
        working += SToStr(value);
        working += separator;
    });
    return working.substr(0, working.size() - separator.size());
}

inline void SAppendToList(string& value, const string& addValue)
{
    list<string> valueList;
    SParseList(value, valueList);
    valueList.push_back(addValue);
    value = SComposeList(valueList);
}

// --------------------------------------------------------------------------
// JSON stuff
// --------------------------------------------------------------------------
// JSON message management
extern string SToJSON(const string& value, const bool forceString = false);
inline string SComposeJSONArray(const vector<string>& valueList)
{
    if (valueList.empty())
        return "[]";
    string working = "[";
    SFOREACHCONST(vector<string>, valueList, valueIt) working += SToJSON(*valueIt) + ",";
    working.resize(working.size() - 1);
    working += "]";
    return working;
}
inline string SComposeJSONArray(const list<string>& valueList)
{
    if (valueList.empty())
        return "[]";
    string working = "[";
    SFOREACHCONST(list<string>, valueList, valueIt) working += SToJSON(*valueIt) + ",";
    working.resize(working.size() - 1);
    working += "]";
    return working;
}
extern string SComposeJSONObject(const STable& nameValueMap, const bool forceString = false);
extern STable SParseJSONObject(const string& object);
extern list<string> SParseJSONArray(const string& array);
inline string SGetJSONArrayFront(const string& jsonArray)
{
    list<string> l = SParseJSONArray(jsonArray);
    return l.empty() ? "" : l.front();
};

// --------------------------------------------------------------------------
// Network stuff
// --------------------------------------------------------------------------

// Converts a sockaddr_in to a string of the form "aaa.bbb.ccc.ddd:port"
inline string SToStr(const sockaddr_in& addr)
{
    return SToStr(inet_ntoa(addr.sin_addr)) + ":" + SToStr(ntohs(addr.sin_port));
}
inline ostream& operator<<(ostream& os, const sockaddr_in& addr)
{
    return os << SToStr(addr);
}

// map of FDs to pollfds
typedef map<int, pollfd> fd_map;
#define SREADEVTS (POLLIN | POLLPRI)
#define SWRITEEVTS (POLLOUT)

// This will add the events specified in `evts` to the events we'll listen for for this socket,
// or, if this socket isn't in our set, it'll add it.
extern void SFDset(fd_map& fdm, int socket, short evts);

// Returns true if *ANY* of the bits in evts are set as returned value for this socket.
// Returns false otherwise, or if this socket isn't in this fd_set, or if evts is 0.
extern bool SFDAnySet(fd_map& fdm, int socket, short evts);

// Socket helpers
extern int S_socket(const string& host, bool isTCP, bool isPort, bool isBlocking);
extern int S_accept(int port, sockaddr_in& fromAddr, bool isBlocking);
extern ssize_t S_recvfrom(int s, char* recvBuffer, int recvBufferSize, sockaddr_in& fromAddr);
extern bool S_recvappend(int s, string& recvBuffer);
inline string S_recv(int s)
{
    string buf;
    S_recvappend(s, buf);
    return buf;
}
extern bool S_sendconsume(int s, string& sendBuffer);
inline bool S_send(int s, string sendBuffer)
{
    S_sendconsume(s, sendBuffer);
    return sendBuffer.empty();
}
extern int S_poll(fd_map& fdm, uint64_t timeout);

// Network helpers
extern string SGetHostName();
extern string SGetPeerName(int s);

// --------------------------------------------------------------------------
// File stuff
// --------------------------------------------------------------------------
// Basic file loading and saving
extern bool SFileExists(const string& path);
extern bool SFileLoad(const string& path, string& buffer);
inline string SFileLoad(const string& path)
{
    string buffer;
    SFileLoad(path, buffer);
    return buffer;
}
extern bool SFileSave(const string& path, const string& buffer);
extern bool SFileCopy(const string& fromPath, const string& toPath);
extern uint64_t SFileSize(const string& path);

// --------------------------------------------------------------------------
// Crypto stuff
// --------------------------------------------------------------------------
// Various hashing functions
extern string SHashSHA1(const string& buffer);

// Various encoding/decoding functions
extern string SEncodeBase64(const string& buffer);
extern string SDecodeBase64(const string& buffer);

// HMAC (for use with Amazon S3)
extern string SHMACSHA1(const string& key, const string& buffer);

// Encryption/Decryption
#define SAES_KEY_SIZE 32 // AES256 32 bytes = 256 bits
#define SAES_BLOCK_SIZE 16
extern string SAESGenerate();
extern string SAESEncrypt(const string& buffer, const string& iv, const string& key);
extern string SAESDecrypt(const string& buffer, const string& iv, const string& key);

// --------------------------------------------------------------------------
// Credit card stuff
// --------------------------------------------------------------------------
// Determine if some input is a PAN
inline bool SIsPAN(const string& value)
{
    return SREMatch("^\\d{13,19}$", value);
}
inline bool SIsMaskedPAN(const string& value)
{
    return SREMatch("^\\d{0,6}[Xx]+\\d{4,7}$", value);
}

// --------------------------------------------------------------------------
// Helper function to mask out the necessary digits in a card number to
// comply with PCI 3.3.  Namely, replace all but the first six and last four
// digits with X.
inline string SMaskPAN(const string& pan)
{
    // First, make sure it's valid
    const string& safePAN = SReplaceAllBut(pan, "0123456789", 'X');

    // Hide these numbers completely.
    // We should not be getting account numbers this small or large.
    if (safePAN.size() < 6 || 20 < safePAN.size())
        return string(safePAN.size(), 'X');

    // Can show last 4.
    if (safePAN.size() < 14)
        return string(safePAN.size() - 4, 'X') + safePAN.substr(safePAN.size() - 4);

    // Can show last 4 and first 6.
    return safePAN.substr(0, 6) + string(safePAN.size() - 10, 'X') + safePAN.substr(safePAN.size() - 4);
}

// --------------------------------------------------------------------------
// SQLite Stuff
// --------------------------------------------------------------------------
#include "sqlite3.h"
#include "SQResult.h"
inline string SQ(const char* val)
{
    return "'" + SEscape(val, "'", '\'') + "'";
}
inline string SQ(const string& val)
{
    return SQ(val.c_str());
}
inline string SQ(int val)
{
    return SToStr(val);
}
inline string SQ(unsigned val)
{
    return SToStr(val);
}
inline string SQ(uint64_t val)
{
    return SToStr(val);
}
inline string SQ(int64_t val)
{
    return SToStr(val);
}
inline string SQ(double val)
{
    return SToStr(val);
}
extern string SQList(const string& val, bool integersOnly = true);

template <typename Container> string SQList(const Container& valueList)
{
    list<string> safeValues;
    for (typename Container::const_iterator valueIt = valueList.begin(); valueIt != valueList.end(); ++valueIt) {
        safeValues.push_back(SQ(*valueIt));
    }
    return SComposeList(safeValues);
}

extern void SQueryLogOpen(const string& logFilename);
extern void SQueryLogClose();
extern bool SQuery(sqlite3* db,
                   const char* e,
                   const string& sql,
                   SQResult& result,
                   int64_t warnThreshold = 1000 * STIME_US_PER_MS);
inline bool SQuery(sqlite3* db, const char* e, const string& sql, int64_t warnThreshold = 1000 * STIME_US_PER_MS)
{
    SQResult ignore;
    return SQuery(db, e, sql, ignore, warnThreshold);
}
#define SSTR(_val_) #_val_
#define __SLINE__ SSTR(__LINE__)
#define SQUERY(_db_, _query_, _result_) SQuery(_db_, __FILE__ __SLINE__, (string)_query_, _result_)
#define SQUERYIGNORE(_db_, _query_) SQuery(_db_, __FILE__ __SLINE__, (string)_query_)
#define SASSERTQUERY(_db_, _query_, _result_) SASSERT(SQUERY(_db_, _query_, _result_))
#define SASSERTQUERYIGNORE(_db_, _query_) SASSERT(SQUERYIGNORE(_db_, _query_))
extern bool SQVerifyTable(sqlite3* db, const string& tableName, const string& sql);

// --------------------------------------------------------------------------
inline string STIMESTAMP(uint64_t when)
{
    return SQ(SComposeTime("%Y-%m-%d %H:%M:%S", when));
}
inline string SCURRENT_TIMESTAMP()
{
    return STIMESTAMP(STimeNow());
}

// --------------------------------------------------------------------------
// Miscellaneous stuff
// --------------------------------------------------------------------------
// Compression
extern string SGZip(const string& content);

// Command-line helpers
extern SData SParseCommandLine(int argc, char* argv[]);

// --------------------------------------------------------------------------
// Testing Stuff
// --------------------------------------------------------------------------
// Logs the start and end of a given test, as well as elapsed time.
#define STESTLOG(_MSG_)                                                                                                \
    do {                                                                                                               \
        cout << _MSG_ << endl;                                                                                         \
        cout.flush();                                                                                                  \
        SSYSLOG(LOG_DEBUG, SWHEREAMI << "[test] " << SLOGPREFIX << _MSG_);                                             \
    } while (false)
#define STESTASSERT(_COND_)                                                                                            \
    do {                                                                                                               \
        if (!(_COND_)) {                                                                                               \
            STESTLOG((test.success ? "\n" : "") << "\tAssertion failed: " << #_COND_);                                 \
            test.success = false;                                                                                      \
        }                                                                                                              \
    } while (false)
#define STESTEQUALS(_LHS_, _RHS_)                                                                                      \
    do {                                                                                                               \
        if ((_LHS_) != (_RHS_)) {                                                                                      \
            STESTLOG((test.success ? "\n" : "") << "\tAssertion failed: (" << #_LHS_ << ") != (" << #_RHS_ << "): ("   \
                                                << _LHS_ << ") != (" << _RHS_ << ")");                                 \
            test.success = false;                                                                                      \
        }                                                                                                              \
    } while (false)
struct STestGroup {
    bool success;
    STestGroup() : success(true){};
};

struct STestTimer {
    // Attributes
    uint64_t begin;
    bool success;
    STestGroup* testGroup;

    // Starts a new test
    STestTimer(const string& message, STestGroup* testGroup = 0)
        : begin(STimeNow()), success(true), testGroup(testGroup)
    {
        // Log to the logfile and output
        SINFO("################# " << message << " ###################");
        cout << message << "...";
        cout.flush();
    }

    // Test complete
    ~STestTimer()
    {
        // Update the test group, if any
        if (testGroup) {
            testGroup->success &= success;
        }

        // Calculate and log elapsed
        uint64_t elapsed = (STimeNow() - begin) / STIME_US_PER_MS;
        string result = (success ? "Pass." : "FAIL!");
        SINFO("################# " << result << " (" << elapsed << "ms)  ###################");
        cout << result << " (" << elapsed << "ms)" << endl;
    }
};

// --------------------------------------------------------------------------
// Networking stuff
// --------------------------------------------------------------------------
// Networking includes
#include "SX509.h"
#include "SSSLState.h"
#include "STCPManager.h"
#include "STCPServer.h"
#include "STCPNode.h"
#include "SDataClient.h"
#include "SHTTPSManager.h"
