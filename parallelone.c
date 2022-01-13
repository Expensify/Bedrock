/*
** 2022-01-02
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file implements a table-valued function that runs multiple
** queries in parallel, each in a separate thread, and returns
** the merged results.
**
** The name of the function is parallelone(X).  The X argument must be a
** JSON string.  The JSON specifies a set of external database files and
** a query to run against each file.  For example:
**
**    SELECT * FROM parallelone('{
**       "tasks":[
**         { "dbname":"/home/www/logs/log-20210101.db",
**           "query": "SELECT ... FROM ... ORDER BY ..." },
**         { "dbname":"/home/www/logs/log-20210101.db",
**           "query": "SELECT ... FROM ... ORDER BY ..." },
**         ...
**         { "dbname":"/home/www/logs/log-20220101.db",
**           "query": "SELECT ... FROM ... ORDER BY ..." }
**        ]
**    }');
**
** The parallelone() function creates a new thread for each "task".  Each
** thread opens a separate read-only connection to the named database file,
** runs the query, and returns the results back up to the parallelone()
** table.  Assuming the results from individual subqueries are sorted,
** parallelone() merges the results such that the combined output is also
** sorted.
**
** LIMITATIONS:
**
**    *    This table-valued function currently only works on unix.
**
**    *    Only the first 5 columns of each subquery are captured and
**         returned.  That constant 5 could be raised, but whatever you
**         raise it to would still be a constant.  We don't have a good
**         way of making the number of columns dynamic.
**
**    *    The result columns from parallelone() are named "c0", "c1",
**         "c2", "c3", and "c4", and they sort in that order. There is also a
**         "taskid" column that has a value from 0 through N-1 indicating
**         which task generated that row.
**
**    *    The main thread assumes that the subqueries each return results
**         as if "ORDER BY c0, c1, c2, c3, c4".  If a subquery does not
**         return rows in that order, the final result will not be
**         sorted correctly.
**
** COMPILING:
**
**    gcc -shared -fPIC parallelone.c -o parallelone.so
**
** ALGORITHM:
**
** Because parallelone() is a table-valued function, the cursor object
** (POneCursor) contains all important state information for each instance
** of the function.  The table object is only used to return error messages.
**
** The POneCursor object contains a priority queue of POneTask objects -
** one POneTask for each task described in the input JSON.  Child threads
** only see the one POneTask object for which they were created.  The
** POneTask object is the only communication mechanism between the main
** thread and child thread.
**
** Basic idea:
**
**    (1)  For each "task" object in the input JSON:
**           (a)  Create a new POneTask object
**           (b)  Start a new thread for that object, but don't (yet)
**                wait for any results.
**    (2)  For each POneTask:
**           (a)  Wait for the first row to become available.  (There may
**                be a short delay while waiting for the first row on the
**                first child thread, but as all other threads are running
**                in parallel, they will have caught up and the main thread
**                should not have to wait long for subsequent children.)
**           (b)  Enter the POneTask into the correct spot in the priority
**                queue based on its first row.
**    (3)  While the priority queue is not empty:
**           (a)  Extract the next row from the first POneTask in the
**                priority queue and return it.
**           (b)  Wait for the next row to appear on that task.  (Since the
**                thread is running in parallel, usually the next row will
**                already be there and we won't really have to wait.)
**                (i)  If the query is done, join the child thread and
**                     remove the POneTask from the priority queue.
**           (c)  Rebalance the priority queue.
**
** DEBUGGING:
**
** Usually each "task" requires two JSON values:
**
**      "dbname"       The name of the database file to open and query.
**      "query"        A single SELECT statement to run against that database.
**
** But for testing and debugging purposes, you can alternatively specify:
**
**      "data":[{"c0":1,"c1":"hello",...},...]
**
** For a "data" task, no child thread is created.  Content is taken
** directly from the JSON, in the order that it appears in the JSON.  Use
** this "data" feature to trouble-shoot or analyze the priority queue
** mechanism.
*/
#if !defined(SQLITEINT_H)
#include "sqlite3ext.h"
#endif
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <pthread.h>

#define PONE_NCOL  5  /* Number of data columns */
#define UNUSED_PARAMETER(x)  (void)x

typedef unsigned char u8;

/*
** A single SQL value.
*/
typedef struct POneValue POneValue;
struct POneValue {
  u8 eType;               /* Datatype, SQLITE_NULL, SQLITE_INTEGER, etc. */
  u8 fromMalloc;          /* True if sqlite3_free() needed for u.z */
  int nByte;              /* Size in bytes for TEXT or BLOB */
  union {
    sqlite3_int64 i;      /* Value for INTEGER */
    double r;             /* Value for REAL */
    char *z;              /* Value for TEXT or BLOB */
  } u;
};


/*
** A linked list of result rows.
*/
typedef struct POneRow POneRow;
struct POneRow {
  POneRow *pNext;                /* Next row */
  POneValue aValue[PONE_NCOL];   /* Values for this row */
};

/*
** The following object represents a single parallel task.
*/
typedef struct POneTask POneTask;
struct POneTask {
  /***** Fields accessible only when holding the mutex *****/
  sqlite3 *db;            /* Child database connection */
  POneRow *pRow;          /* List of available results */
  POneRow *pLast;         /* Last row in the pRow list */
  int isDone;             /* True when the query completes */
  char *zErrMsg;          /* Error message text.  From sqlite3_mprintf() */

  /***** Fields accessible by the main thread only *****/
  sqlite3 *dbMain;        /* The database connection for the main thread */
  char *zData;            /* JSON data for a data-only task */
  POneRow *pCurrent;      /* Current row */
  int iTaskId;            /* Task ID */
  int bThreaded;          /* True for threaded tasks */
  
  /***** Fields accessible by the child thread only *****/
  sqlite3 *dbChld;        /* Copy of db, copied outside the mutex */
  char *zDbName;          /* Name of the database file in which to run query */
  char *zSql;             /* SQL text to run */

  /***** Thread control.  Only valid if bThreaded is true *****/
  pthread_t chldThread;   /* The child thread */
  pthread_mutex_t sMutex; /* The access mutex */
  pthread_cond_t sChng;   /* Used to wait for changes */
};

/* POneVtab is a subclass of sqlite3_vtab which is
** underlying representation of the virtual table
*/
typedef struct POneVtab POneVtab;
struct POneVtab {
  sqlite3_vtab base;      /* Base class - must be first */
  /* Add new fields here, as necessary */
  sqlite3 *db;            /* The database connection that owns this table */
};

/* POneCursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct POneCursor POneCursor;
struct POneCursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  /* Insert new fields here.  For this parallelOne we only keep track
  ** of the rowid */
  sqlite3 *db;               /* Database connection of this cursor */
  sqlite3_vtab *pBase;       /* Convenience pointer to parent vtab class */
  sqlite3_int64 iRowid;      /* The rowid */
  int nTask;                 /* Current number of tasks */
  int nPending;              /* Extra uninitialized tasks */
  int nAlloc;                /* Number of slots allocated for apTask[] */
  POneTask **apTask;         /* Priority queue of tasks */
};


/******************* Internal Utility Routines ********************************/


/* Compare two POneValue objects.  Return negative, zero, or positive if
** the first is less then, equal to, or greater than the second.
*/
static int pvalueCompare(const POneValue *pLeft, const POneValue *pRight){
  switch( pLeft->eType ){
    case SQLITE_NULL: {
      return pRight->eType==SQLITE_NULL ? 0 : -1;
    }
    case SQLITE_INTEGER:
    case SQLITE_FLOAT: {
      if( pRight->eType==SQLITE_NULL ) return +1;
      if( pRight->eType!=SQLITE_INTEGER && pRight->eType!=SQLITE_FLOAT ){
        return -1;
      }
      if( pLeft->eType==SQLITE_INTEGER ){
        long double x;
        if( pRight->eType==SQLITE_INTEGER ){
          if( pLeft->u.i<pRight->u.i ) return -1;
          if( pLeft->u.i>pRight->u.i ) return +1;
          return 0;
        }
        x = (long double)pLeft->u.i;
        if( x<pRight->u.r ) return -1;
        if( x>pRight->u.r ) return +1;
        return 0;
      }else{
        long double x;
        if( pRight->eType==SQLITE_FLOAT ){
          if( pLeft->u.r<pRight->u.r ) return -1;
          if( pLeft->u.r>pRight->u.r ) return +1;
          return 0;
        }
        x = (long double)pRight->u.i;
        if( pLeft->u.r<x ) return -1;
        if( pLeft->u.r>x ) return +1;
        return 0;
      }
    }
    case SQLITE_TEXT: {
      int mn, c;
      if( pRight->eType==SQLITE_BLOB ) return -1;
      if( pRight->eType!=SQLITE_TEXT ) return +1;
      mn = pLeft->nByte;
      if( mn>pRight->nByte ) mn = pRight->nByte;
      c = memcmp(pLeft->u.z, pRight->u.z, mn);
      if( c==0 ){
        c = pLeft->nByte - pRight->nByte;
      }
      return c;
    }
    case SQLITE_BLOB: {
      int mn, c;
      if( pRight->eType!=SQLITE_BLOB ) return +1;
      mn = pLeft->nByte;
      if( mn>pRight->nByte ) mn = pRight->nByte;
      c = memcmp(pLeft->u.z, pRight->u.z, mn);
      if( c==0 ){
        c = pLeft->nByte - pRight->nByte;
      }
      return c;
    }
  }
  return 0;  /* Not reached */
}

/* Reclaim memory allocated to old a TEXT or BLOB in a POneValue.
** Leave the POneValue set to NULL.
*/
static void pvalueClear(POneValue *p){
  if( p->fromMalloc ){
    sqlite3_free(p->u.z);
  }
  p->eType = SQLITE_NULL;
}

/*
** Delete an entire linked list of POneRows.
*/
static void prowDelete(POneRow *pRow){
  while( pRow ){
    POneRow *pNext = pRow->pNext;
    int i;
    for(i=0; i<PONE_NCOL; i++) pvalueClear(&pRow->aValue[i]);
    sqlite3_free(pRow);
    pRow = pNext;
  }
}

/*
** Transfer content for the first five columns of the current row in pStmt
** into a new POneRow object.  Return a pointer to the new POneRow object.
** Or, return NULL or an OOM error.
*/
static POneRow *prowFromStmt(sqlite3_stmt *pStmt){
  POneRow *pRow = sqlite3_malloc( sizeof(*pRow) );
  int i;
  int n;
  int rc = SQLITE_OK;
  if( pRow==0 ) return 0;
  memset(pRow, 0, sizeof(*pRow));
  n = sqlite3_data_count(pStmt);
  for(i=0; rc==SQLITE_OK && i<PONE_NCOL; i++){
    u8 eType;
    if( i>=n ){
      pRow->aValue[i].eType = SQLITE_NULL;
      continue;
    }
    eType = pRow->aValue[i].eType = sqlite3_column_type(pStmt,i);
    switch( eType ){
      case SQLITE_INTEGER: {
        pRow->aValue[i].u.i = sqlite3_column_int64(pStmt,i);
        break;
      }
      case SQLITE_FLOAT: {
        pRow->aValue[i].u.r = sqlite3_column_double(pStmt,i);
        break;
      }
      case SQLITE_BLOB:
      case SQLITE_TEXT: {
        int n = sqlite3_column_bytes(pStmt, i);
        char *z;
        const char *zData;
        if( eType==SQLITE_TEXT ){
          zData = (const char*)sqlite3_column_text(pStmt, i);
        }else{
          zData = (const char*)sqlite3_column_blob(pStmt, i);
        }
        if( zData==0 ){
          pRow->aValue[i].eType = SQLITE_NULL;
          rc = SQLITE_NOMEM;
          break;
        }
        pRow->aValue[i].nByte = n;
        pRow->aValue[i].u.z = z = sqlite3_malloc64( n+1 );
        if( z==0 ){
          rc = SQLITE_NOMEM;
          pRow->aValue[i].eType = SQLITE_NULL;
        }else{
          z[n] = 0;
          memcpy(z, zData, n);
        }
        break;
      }
    }
  }
  if( rc!=SQLITE_OK ){
    prowDelete(pRow);
    pRow = 0;
  }
  return pRow;
}

/*
** Compare two POneRow objects.  Return negative, zero, or positive if the
** first is less than, equal to, or greater than the second.
*/
static int prowCompare(const POneRow *pFirst, const POneRow *pSecond){
  int c = 0;
  int i;
  for(i=0; i<PONE_NCOL; i++){
    c = pvalueCompare(&pFirst->aValue[i], &pSecond->aValue[i]);
    if( c ) break;
  }
  return c;
}

/*
** Add error message text to the POneCursor
*/
static void pcursorError(POneCursor *pCur, const char *zFormat, ...){
  va_list ap;
  sqlite3_free(pCur->pBase->zErrMsg);
  va_start(ap, zFormat);
  pCur->pBase->zErrMsg = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
}

/*
** Compare the values in the next available ros of two POneTask objects.
** Return negative, zero, or positive if the first task is less than,
** equal to, or greater than the second.
*/
static int ptaskCompare(const POneTask *pA, const POneTask *pB){
  int c;
  c = prowCompare(pA->pCurrent, pB->pCurrent);
  if( c==0 ) c = pA->iTaskId - pB->iTaskId;
  return c;
}

#if 0
/*
** For debugging, print out the task queue
*/
static void pqueuePrint(POneCursor *pCur){
  int i;
  for(i=0; i<pCur->nTask; i++){
    printf("[%02d]: #%d\n", i, pCur->apTask[i]->iTaskId);
  }
  for(; i<pCur->nTask+pCur->nPending; i++){
    printf("[%02d]: (PENDING) #%d\n", i, pCur->apTask[i]->iTaskId);
  }
}
#endif

/*
** The first POneCur.nTask entries of the POneCur.apTask[] array form a
** priority queue.  That is to say, for any index I where I>0, the
** POneTask.pCurrent value for entry (I-1)/2 is less than or equal to the
** pCurrent value for I itself.
**
** The pCurrent value for entry "idx" has changed.  the job of this routine
** is to restore the priority query property for lower-number entries.
*/
static void pqueueBalanceDownward(POneCursor *pCur, int idx){
  while( idx>0 ){
    POneTask *pTemp;
    int parent = (idx-1)/2;
    int c = ptaskCompare(pCur->apTask[parent],pCur->apTask[idx]);
    if( c<=0 ) break;
    pTemp = pCur->apTask[parent];
    pCur->apTask[parent] = pCur->apTask[idx];
    pCur->apTask[idx] = pTemp;
    idx = parent;
  }    
}

/*
** In this case, the pCurrent value for the "idx"-th task has changed.
** Make sure larger values are still correct.
*/
static void pqueueBalanceUpward(POneCursor *pCur, int idx){
  while( idx*2+1 < pCur->nTask ){
    int c1, w = 1;
    POneTask *pTemp;
    if( idx*2+2 < pCur->nTask
     && ptaskCompare(pCur->apTask[idx*2+2],pCur->apTask[idx*2+1])<0
    ){
      w = 2;
    }
    c1 = ptaskCompare(pCur->apTask[idx],pCur->apTask[idx*2+w]);
    if( c1<=0 ) break;
    pTemp = pCur->apTask[idx];
    pCur->apTask[idx] = pCur->apTask[idx*2+w];
    pCur->apTask[idx*2+w] = pTemp;
    idx = idx*2+w;
  }
}

/*
** Create a new task object and insert it as a pending task in the
** POneCursor.  Return the index in pCur->apTask[] for the new task.
** Or, return -1 if there is an error.
*/
static int ptaskNew(POneCursor *pCur){
  POneTask *pTask;
  if( pCur->nTask + pCur->nPending >= pCur->nAlloc ){
    int nNewAlloc = 2*pCur->nAlloc + 10;
    POneTask **apTask;
    apTask = sqlite3_realloc(pCur->apTask, nNewAlloc*sizeof(POneTask*));
    if( apTask==0 ) return -1;
    pCur->apTask = apTask;
    pCur->nAlloc = nNewAlloc;
  }
  pTask = sqlite3_malloc( sizeof(*pTask) );
  if( pTask==0 ) return -1;
  memset(pTask, 0, sizeof(*pTask));
  pTask->dbMain = pCur->db;
  pTask->iTaskId = pCur->nTask+pCur->nPending+1;
  pCur->apTask[pCur->nTask+pCur->nPending] = pTask;
  return pCur->nTask + pCur->nPending++;
}

/*
** Delete a POneTask object.  Reclaim all allocated memory.
**
** If there was an error reported (in the POneTask.zErrMsg field) and
** if pCur is not NULL, then transfer the error to that cursor and
** return SQLITE_ERROR.  Otherwise return SQLITE_OK;
*/
static int ptaskDelete(POneTask *pTask, POneCursor *pCur){
  int rc = SQLITE_OK;
  if( pTask==0 ) return SQLITE_OK;
  if( pTask->bThreaded ){
    pthread_mutex_lock(&pTask->sMutex);
    if( pTask->db ) sqlite3_interrupt(pTask->db);
    pthread_mutex_unlock(&pTask->sMutex);
    pthread_join(pTask->chldThread, 0);
    pthread_mutex_destroy(&pTask->sMutex);
    pthread_cond_destroy(&pTask->sChng);
  }
  if( pTask->zErrMsg ){
    if( pCur ){
      sqlite3_free(pCur->pBase->zErrMsg);
      pCur->pBase->zErrMsg = pTask->zErrMsg;
      rc = SQLITE_ERROR;
    }else{
      sqlite3_free(pTask->zErrMsg);
    }
  }   
  sqlite3_free(pTask->zDbName);
  sqlite3_free(pTask->zSql);
  sqlite3_free(pTask->zData);
  prowDelete(pTask->pRow);
  prowDelete(pTask->pCurrent);
  sqlite3_free(pTask);
  return rc;
}

/*
** Add a new pending POneTask to the POneCursor based on a JSON array.  The
** JSON looks like this:
**
**   [{"c0":1,"c1":...},{...},...]
*/
static int ptaskNewFromData(POneCursor *pCur, const char *zData){
  POneTask *pTask;
  int i;

  i = ptaskNew(pCur);
  if( i<0 ) return SQLITE_NOMEM;
  assert( i==pCur->nTask+pCur->nPending-1 );
  assert( pCur->nPending>0 );
  pTask = pCur->apTask[i];
  pTask->zData = sqlite3_mprintf("%s", zData);
  if( pTask->zData==0 ){
    ptaskDelete(pTask, 0);
    pCur->nPending--;
    return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

/*
** An error has occurred in the child thread.  Write the error message
** and shutdown processing in the child thread.
*/
static void pchildFatal(POneTask *pTask, const char *zFormat, ...){
  char *zErr;
  va_list ap;

  va_start(ap, zFormat);
  zErr = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  pthread_mutex_lock(&pTask->sMutex);
  sqlite3_free(pTask->zErrMsg);
  pTask->zErrMsg = zErr;
  pTask->isDone = 1;
  pTask->db = 0;
  pthread_cond_signal(&pTask->sChng);
  pthread_mutex_unlock(&pTask->sMutex);
  if( pTask->dbChld ){
    sqlite3_close(pTask->dbChld);
    pTask->dbChld = 0;
  }
}

/*
** This is the main routine for a child thread.
**
** The main thread has already set of the POneTask object with the name
** of the database file and the query.  This routine just has to open
** the file and run the query and queue the results.
*/
static void *pchildMain(void *pArg){
  POneTask *pTask = (POneTask*)pArg;
  int rc;
  sqlite3_stmt *pStmt;
  
  /* Open the database */
  pTask->dbChld = 0;
  rc = sqlite3_open_v2(pTask->zDbName, &pTask->dbChld, SQLITE_OPEN_READONLY,0);
  if( rc!=0 ){
    pchildFatal(pTask, "unable to open database \"%s\"", pTask->zDbName);
    return 0;
  }
  pthread_mutex_lock(&pTask->sMutex);
  assert( pTask->pRow==0 );
  assert( pTask->pLast==0 );
  assert( pTask->isDone==0 );
  assert( pTask->zErrMsg==0 );
  pTask->db = pTask->dbChld;
  pthread_mutex_unlock(&pTask->sMutex);

  /* Compile the query */
  pStmt = 0;
  rc = sqlite3_prepare_v2(pTask->dbChld, pTask->zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK || pStmt==0 ){
    sqlite3_finalize(pStmt);
    pchildFatal(pTask, "error during prepare of \"%s\": %s", pTask->zSql,
                sqlite3_errmsg(pTask->dbChld));
    return 0;
  }

  /* Return query results */
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    POneRow *pRow = prowFromStmt(pStmt);
    if( pRow==0 ){
      sqlite3_finalize(pStmt);
      pchildFatal(pTask, "out of memory");
      return 0;
    }
    pthread_mutex_lock(&pTask->sMutex);
    if( pTask->pLast ){
      pTask->pLast->pNext = pRow;
    }else{
      pTask->pRow = pRow;
    }
    pTask->pLast = pRow;
    pthread_cond_signal(&pTask->sChng);
    pthread_mutex_unlock(&pTask->sMutex);
  }
  sqlite3_finalize(pStmt);

  /* Final shutdown of the child */
  pthread_mutex_lock(&pTask->sMutex);
  pTask->isDone = 1;
  pTask->db = 0;
  pthread_cond_signal(&pTask->sChng);
  pthread_mutex_unlock(&pTask->sMutex);
  sqlite3_close(pTask->dbChld);
  pTask->dbChld = 0;

  return 0;
}

/*
** Add a new pending POneTask to the POneCursor that will attempt to
** execute SQL in a separate thread.
*/
static int ptaskNewFromFileAndQuery(
  POneCursor *pCur,      /* Add the new task to this cursor */
  const char *zDbName,   /* Database filename */
  const char *zSql       /* Query to run against the database */
){
  POneTask *pTask;
  int i;
  int rc;

  i = ptaskNew(pCur);
  if( i<0 ) return SQLITE_NOMEM;
  assert( i==pCur->nTask+pCur->nPending-1 );
  assert( pCur->nPending>0 );
  pTask = pCur->apTask[i];
  pTask->zDbName = sqlite3_mprintf("%s", zDbName);
  pTask->zSql = sqlite3_mprintf("%s", zSql);
  if( pTask->zDbName==0 || pTask->zSql==0 ){
    ptaskDelete(pTask, 0);
    pCur->nPending--;
    return SQLITE_NOMEM;
  }
  /* Start the thread */
  pthread_mutex_init(&pTask->sMutex, 0);
  pthread_cond_init(&pTask->sChng, 0);
  pTask->bThreaded = 1;
  rc = pthread_create(&pTask->chldThread, 0, pchildMain, pTask);
  if( rc ){
    pTask->bThreaded = 0;
    pthread_mutex_destroy(&pTask->sMutex);
    pthread_cond_destroy(&pTask->sChng);
    pcursorError(pCur, "unable to create a thread for task #%d",
                 pTask->iTaskId);
    rc = SQLITE_ERROR;
  }
  return rc;
}

/*
** Bring a task up to the point where data is available in the
** POneTask.pCurrent, or until the task has reached the end of
** its content. Return an error code.
**
** If new further data is available, this routine still returns SQLITE_OK.
** It just leaves pCurrent set to NULL.
**
** The child thread stores content in the POneTask.pRow list.  This
** function runs in the main thread and transfers content from 
** POneTask.pRow over into POneTask.pCurrent.  All content that has
** accumulated in POneTask.pRow is transferred over to pCurrent, all
** in one go.  Typically, the child will have generated multiple rows
** of result.  This routine transfers them all to the pCurrent where
** they will be accessible to the main thread.  This routine will
** not be invoked again until the pCurrent list has been consumed.
*/
static int ptaskWaitForData(POneTask *pTask, POneCursor *pCur){
  int rc = SQLITE_OK;
  assert( pTask->pCurrent==0 );
  if( pTask->bThreaded ){
    char *zErrMsg = 0;
    pthread_mutex_lock(&pTask->sMutex);
    while( pTask->pRow==0
        && pTask->isDone==0
        && pTask->zErrMsg==0 ){
      pthread_cond_wait(&pTask->sChng, &pTask->sMutex);
    }
    pTask->pCurrent = pTask->pRow;
    pTask->pRow = 0;
    pTask->pLast = 0;
    zErrMsg = pTask->zErrMsg;
    pTask->zErrMsg = 0;
    pthread_mutex_unlock(&pTask->sMutex);
    if( zErrMsg ){
      pcursorError(pCur, "parallelone thread #%d: %z", pTask->iTaskId, zErrMsg);
      rc = SQLITE_ERROR;
    }
  }
  if( pTask->zData ){
    sqlite3_stmt *pStmt = 0;
    char *zSql;
    POneRow *pLast = 0;
    zSql = sqlite3_mprintf("SELECT "
      " json_extract(value,'$.c0'),"
      " json_extract(value,'$.c1'),"
      " json_extract(value,'$.c2'),"
      " json_extract(value,'$.c3'),"
      " json_extract(value,'$.c4')"
      "FROM json_each(%Q)", pTask->zData);
    sqlite3_free(pTask->zData);
    pTask->zData = 0;
    if( zSql==0 ) return SQLITE_NOMEM;
    rc = sqlite3_prepare_v2(pTask->dbMain, zSql, -1, &pStmt, 0);
    sqlite3_free(zSql);
    while( rc==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
      POneRow *pRow = prowFromStmt(pStmt);
      if( pRow==0 ){
        rc = SQLITE_NOMEM;
        break;
      }
      if( pLast ){
        pLast->pNext = pRow;
      }else{
        pTask->pCurrent = pRow;
      }
      pLast = pRow;
    }
    sqlite3_finalize(pStmt);
  }
  return rc;
}

/*
** Reset a cursor by clearing out all tasks and reclaiming all memory.
** Don't delete the POneCursor object itself, just the content that
** it holds.
*/
static void pcursorReset(POneCursor *pCur){
  int n = pCur->nTask+pCur->nPending;
  int i;
  for(i=0; i<n; i++){
    ptaskDelete(pCur->apTask[i], 0);
  }
  sqlite3_free(pCur->apTask);
  pCur->apTask = 0;
  pCur->nAlloc = 0;
  pCur->nTask = 0;
  pCur->nPending = 0;
  pCur->iRowid = 0;
}


/***************************   Virtual Table Methods  *************************/
/*
** The parallelOneConnect() method is invoked to create a new
** template virtual table.
**
** Think of this routine as the constructor for POneVtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the POneVtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against the virtual table will look like.
*/
static int parallelOneConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  POneVtab *pNew;
  int rc;
  UNUSED_PARAMETER(pAux);
  UNUSED_PARAMETER(argc);
  UNUSED_PARAMETER(argv);
  UNUSED_PARAMETER(pzErr);

  rc = sqlite3_declare_vtab(db,
           "CREATE TABLE x(c0,c1,c2,c3,c4,taskid,json_spec HIDDEN)"
       );
#define PONE_TASKID 5   /* Column number for "taskid" */
#define PONE_SPEC   6   /* Column number for "json_spec" */
  if( rc==SQLITE_OK ){
    pNew = sqlite3_malloc( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
    pNew->db = db;
  }
  return rc;
}

/*
** This method is the destructor for POneVtab objects.
*/
static int parallelOneDisconnect(sqlite3_vtab *pVtab){
  POneVtab *p = (POneVtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new POneCursor object.
*/
static int parallelOneOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  POneVtab *pVTab = (POneVtab*)p;
  POneCursor *pCur;
  UNUSED_PARAMETER(p);
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  pCur->db = pVTab->db;
  pCur->pBase = p;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a POneCursor.
*/
static int parallelOneClose(sqlite3_vtab_cursor *cur){
  POneCursor *pCur = (POneCursor*)cur;
  pcursorReset(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}


/*
** Advance a POneCursor to its next row of output.
*/
static int parallelOneNext(sqlite3_vtab_cursor *cur){
  POneCursor *pCur = (POneCursor*)cur;
  POneTask *pTop;
  POneRow *pThisRow, *pNextRow;
  int rc = SQLITE_OK;

  assert( pCur->nPending==0 );
  if( pCur->nTask==0 ) return SQLITE_OK;
  pTop = pCur->apTask[0];
  pThisRow = pTop->pCurrent;
  assert( pThisRow!=0 );
  pNextRow = pThisRow->pNext;
  pThisRow->pNext = 0;
  prowDelete(pThisRow);
  pTop->pCurrent = pNextRow;
  if( pNextRow==0 ){
    ptaskWaitForData(pTop, pCur);
    if( pTop->pCurrent==0 ){
      rc = ptaskDelete(pTop, pCur);
      pCur->nTask--;
      if( pCur->nTask>0 ){
        pTop = pCur->apTask[pCur->nTask];
        pCur->apTask[0] = pTop;
      }
    }
  }
  pqueueBalanceUpward(pCur, 0);
    
  pCur->iRowid++;
  return rc;
}

/*
** Return values of columns for the row at which the POneCursor
** is currently pointing.
*/
static int parallelOneColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  POneCursor *pCur = (POneCursor*)cur;
  if( i>=0 && i<PONE_NCOL && pCur->nTask>0 ){
    POneValue *pVal;
    assert( pCur->apTask[0]!=0 );
    assert( pCur->apTask[0]->pCurrent!=0 );
    pVal = &pCur->apTask[0]->pCurrent->aValue[i];
    switch( pVal->eType ){
      case SQLITE_INTEGER: {
        sqlite3_result_int64(ctx, pVal->u.i);
        break;
      }
      case SQLITE_FLOAT: {
        sqlite3_result_double(ctx, pVal->u.r);
        break;
      }
      case SQLITE_TEXT: {
        sqlite3_result_text64(ctx, pVal->u.z, pVal->nByte, SQLITE_TRANSIENT,
                              SQLITE_UTF8);
        break;
      }
      case SQLITE_BLOB: {
        sqlite3_result_blob64(ctx, pVal->u.z, pVal->nByte, SQLITE_TRANSIENT);
        break;
      }
    }
  }else if( i==PONE_NCOL && pCur->nTask>0 ){
    sqlite3_result_int(ctx, pCur->apTask[0]->iTaskId);
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.
*/
static int parallelOneRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  POneCursor *pCur = (POneCursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int parallelOneEof(sqlite3_vtab_cursor *cur){
  POneCursor *pCur = (POneCursor*)cur;
  return pCur->nTask==0;
}

/*
** This method provides the JSON task list to the POneCursor object.
** This method parses the task list, creates all appropriate POneTask
** objects, and starts up child threads.
*/
static int parallelOneFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  POneCursor *pCur = (POneCursor*)pVtabCursor;
  const char *zSpec;
  int rc;
  sqlite3_stmt *pStmt = 0;
  char *zSql;

  UNUSED_PARAMETER(idxStr);
  assert( pCur->nTask==0 );
  pcursorReset(pCur);
  if( idxNum==0 ) return SQLITE_OK;
  assert( argc==1 );
  zSpec = (const char*)sqlite3_value_text(argv[0]);

  /* Verify that zSpec is valid JSON */
  zSql = sqlite3_mprintf("SELECT json_valid(%Q)", zSpec);
  rc = sqlite3_prepare_v2(pCur->db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if( rc==SQLITE_OK &&
   (sqlite3_step(pStmt)!=SQLITE_ROW || sqlite3_column_int(pStmt,0)==0)
  ){
    rc = SQLITE_ERROR;
  }
  sqlite3_finalize(pStmt);
  if( rc ){
    pcursorError(pCur, "invalid JSON for the parallelone spec");
    return rc;
  }


  /* Process the tasks: array from zSpec */  
  zSql = sqlite3_mprintf(
    "SELECT\n"
    "  json_extract(x.value,'$.dbfile') AS dbfile,\n"
    "  json_extract(x.value,'$.query') AS query,\n"
    "  json_extract(x.value,'$.data') AS data\n"
    " FROM json_each(%Q,'$.tasks') AS x",
    zSpec
  );
  rc = sqlite3_prepare_v2(pCur->db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    pcursorError(pCur, "while preparing \"%s\": %s",
                       zSql, sqlite3_errmsg(pCur->db));
  }
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ){
    sqlite3_finalize(pStmt);
    return rc;
  }
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zDbfile = (const char*)sqlite3_column_text(pStmt, 0);
    const char *zQuery = (const char*)sqlite3_column_text(pStmt, 1);
    const char *zData = (const char*)sqlite3_column_text(pStmt, 2);
    if( zDbfile && zDbfile[0] && zQuery && zQuery[0] ){
      rc = ptaskNewFromFileAndQuery(pCur, zDbfile, zQuery);
    }else if( zData && zData[0] ){
      rc = ptaskNewFromData(pCur, zData);
    }
    if( rc ) break;
  }
  sqlite3_finalize(pStmt);

  while( rc==SQLITE_OK && pCur->nPending>0 ){
    POneTask *pTask = pCur->apTask[pCur->nTask];
    rc = ptaskWaitForData(pTask, pCur);
    if( rc ) break;
    if( pTask->pCurrent==0 ){
      pCur->nPending--;
      if( pCur->nPending ){
        memmove(pCur->apTask+pCur->nTask, pCur->apTask+pCur->nTask+1,
                sizeof(POneTask*)*pCur->nPending);
      }
    }else{
      pCur->nTask++;
      pCur->nPending--;
      pqueueBalanceDownward(pCur, pCur->nTask-1);
    }
  }
  if( rc ){
    pcursorReset(pCur);
  }
  pCur->iRowid = 1;
  return rc;
}

/*
** This routine is called by the query planner to evaluate whether a
** particular invocation strategy for parallelone() is possible.
**
** idxNum==0    We do not have an equality constraint on json_spec column.
** idxNum==1    We do havean equlaity constraint on json_spec.
*/
static int parallelOneBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;
  UNUSED_PARAMETER(tab);
  pIdxInfo->idxNum = 0;
  for(i=0; i<pIdxInfo->nConstraint; i++){
    if( pIdxInfo->aConstraint[i].usable==0 ) continue;
    if( pIdxInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pIdxInfo->aConstraint[i].iColumn!=PONE_SPEC ) continue;
    pIdxInfo->idxNum = 1;
    pIdxInfo->aConstraintUsage[i].omit = 1;
    pIdxInfo->aConstraintUsage[i].argvIndex = 1;
    break;
  }
  if( pIdxInfo->nOrderBy>0 ){
    for(i=0; i<pIdxInfo->nOrderBy; i++){
      if( pIdxInfo->aOrderBy[i].desc!=0 ) break;
      if( pIdxInfo->aOrderBy[i].iColumn!=i ) break;
    }
    if( i==pIdxInfo->nOrderBy ){
      pIdxInfo->orderByConsumed = 1;
    }
  }
  pIdxInfo->estimatedCost = (double)2000000000;
  pIdxInfo->estimatedRows = 10000000;
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** virtual table.
*/
static sqlite3_module parallelOneModule = {
  /* iVersion    */ 0,
  /* xCreate     */ 0,
  /* xConnect    */ parallelOneConnect,
  /* xBestIndex  */ parallelOneBestIndex,
  /* xDisconnect */ parallelOneDisconnect,
  /* xDestroy    */ 0,
  /* xOpen       */ parallelOneOpen,
  /* xClose      */ parallelOneClose,
  /* xFilter     */ parallelOneFilter,
  /* xNext       */ parallelOneNext,
  /* xEof        */ parallelOneEof,
  /* xColumn     */ parallelOneColumn,
  /* xRowid      */ parallelOneRowid,
  /* xUpdate     */ 0,
  /* xBegin      */ 0,
  /* xSync       */ 0,
  /* xCommit     */ 0,
  /* xRollback   */ 0,
  /* xFindMethod */ 0,
  /* xRename     */ 0,
  /* xSavepoint  */ 0,
  /* xRelease    */ 0,
  /* xRollbackTo */ 0
};


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_parallelone_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  UNUSED_PARAMETER(pzErrMsg);
  rc = sqlite3_create_module(db, "parallelOne", &parallelOneModule, 0);
  return rc;
}
