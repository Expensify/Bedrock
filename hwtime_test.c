

typedef unsigned long long sqlite3_uint64;
typedef unsigned long long sqlite_uint64;

sqlite_uint64 sqlite3Hwtime(void);

#if defined(_MSC_VER) && defined(_WIN32)

  #include "windows.h"
  #include <profileapi.h>

  __inline sqlite3_uint64 sqlite3Hwtime(void){
    LARGE_INTEGER tm;
    QueryPerformanceCounter(&tm);
    return (sqlite3_uint64)tm.QuadPart;
  }

#elif !defined(__STRICT_ANSI__) && defined(__GNUC__) && \
    (defined(i386) || defined(__i386__) || defined(_M_IX86))

  __inline__ sqlite_uint64 sqlite3Hwtime(void){
     unsigned int lo, hi;
     __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
     return (sqlite_uint64)hi << 32 | lo;
  }

#elif !defined(__STRICT_ANSI__) && (defined(__GNUC__) && defined(__x86_64__))

  __inline__ sqlite_uint64 sqlite3Hwtime(void){
     unsigned int lo, hi;
     __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
     return (sqlite_uint64)hi << 32 | lo;
  }

#elif !defined(__STRICT_ANSI__) && defined(__GNUC__) &&  defined(__aarch64__)

  __inline__ sqlite_uint64 sqlite3Hwtime(void){
     sqlite3_uint64 cnt;
     __asm__ __volatile__ ("mrs %0, cntvct_el0" : "=r" (cnt));
     return cnt;
  }
 
#elif !defined(__STRICT_ANSI__) && (defined(__GNUC__) && defined(__ppc__))

  __inline__ sqlite_uint64 sqlite3Hwtime(void){
      unsigned long long retval;
      unsigned long junk;
      __asm__ __volatile__ ("\n\
          1:      mftbu   %1\n\
                  mftb    %L0\n\
                  mftbu   %0\n\
                  cmpw    %0,%1\n\
                  bne     1b"
                  : "=r" (retval), "=r" (junk));
      return retval;
  }

#else

  /*
  ** asm() is needed for hardware timing support.  Without asm(),
  ** disable the sqlite3Hwtime() routine.
  **
  ** sqlite3Hwtime() is only used for some obscure debugging
  ** and analysis configurations, not in any deliverable, so this
  ** should not be a great loss.
  */
  sqlite_uint64 sqlite3Hwtime(void){ return ((sqlite_uint64)0); }

#endif

#include <stdio.h>

int main(int argc, char **argv){
  int iter = 0;

  for(iter=0; iter<5; iter++){
    int res = 0;
    int ii;
    sqlite3_uint64 t = sqlite3Hwtime();
    for(ii=0; ii<1000000; ii++){
      res += ii;
    }
    printf("%d: result=%d, cycles=%lld\n", iter, res, sqlite3Hwtime() - t);
  }
  return 0;
}




