/*
** 2023 May 16
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/



#ifndef SQLITE3HCT_H
#define SQLITE3HCT_H

/*
** Make sure we can call this stuff from C++.
*/
#ifdef __cplusplus
extern "C" {
#endif

#define SQLITE_HCT_JOURNAL_HASHSIZE 16

/*
** Initialize the main database for replication.
*/
int sqlite3_hct_journal_init(sqlite3 *db);

/* 
** Candidate values for second arg to sqlite3_hct_journal_setmode() 
*/
#define SQLITE_HCT_NORMAL   0
#define SQLITE_HCT_FOLLOWER 1
#define SQLITE_HCT_LEADER   2

/*
** Query the NORMAL/FOLLOWER/LEADER setting of the db passed as the 
** only argument.
*/
int sqlite3_hct_journal_mode(sqlite3 *db);

/*
** Set the LEADER/FOLLOWER setting of the db passed as the first argument.
** Return SQLITE_OK if successful. Otherwise, return an SQLite error code
** and leave an English language error message (accessible using
** sqlite3_errmsg()) in the database handle.
*/
int sqlite3_hct_journal_setmode(sqlite3 *db, int eMode);

/*
** Commit a leader transaction.
*/
int sqlite3_hct_journal_leader_commit(
  sqlite3 *db,                    /* Commit transaction for this db handle */
  const unsigned char *aData,     /* Data to write to "query" column */
  int nData,                      /* Size of aData[] in bytes */
  sqlite3_int64 *piCid,           /* OUT: CID of committed transaction */
  sqlite3_int64 *piSnapshot       /* OUT: Min. snapshot to recreate */
);

/*
** Commit a follower transaction.
*/
int sqlite3_hct_journal_follower_commit(
  sqlite3 *db,                    /* Commit transaction for this db handle */
  const unsigned char *aData,     /* Data to write to "query" column */
  int nData,                      /* Size of aData[] in bytes */
  sqlite3_int64 iCid,             /* CID of committed transaction */
  sqlite3_int64 iSnapshot         /* Value for hct_journal.snapshot field */
);

/*
** Set output variable (*piCid) to the CID of the newest available 
** database snapshot. Return SQLITE_OK if successful, or an SQLite
** error code if something goes wrong.
*/
int sqlite3_hct_journal_snapshot(sqlite3 *db, sqlite3_int64 *piCid);

/*
** Commit a local transaction in either FOLLOWER or LEADER mode. See
** documentation for caveats regarding local transactions.
*/
int sqlite3_hct_journal_local_commit(sqlite3 *db);


/*
** Testing APIs
*/
int sqlite3_hct_cas_failure(int nCASFailCnt, int nCASFailReset);
int sqlite3_hct_io_failure(int nIoFailCnt, int nIoFailReset);
int sqlite3_hct_page_failure(int nPageFailCnt, int nPageFailReset);
void sqlite3_hct_proc_failure(int nProcFailCnt);


#ifdef __cplusplus
}
#endif
#endif /* SQLITE3HCT_H */
