#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#if 0
static void rot13func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zIn;
  int nIn;
  unsigned char *zOut;
  char *zToFree = 0;
  int i;
  char zTemp[100];
  assert( argc==1 );
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  zIn = (const unsigned char*)sqlite3_value_text(argv[0]);
  nIn = sqlite3_value_bytes(argv[0]);
  if( nIn<sizeof(zTemp)-1 ){
    zOut = zTemp;
  }else{
    zOut = zToFree = sqlite3_malloc( nIn+1 );
    if( zOut==0 ){
      sqlite3_result_error_nomem(context);
      return;
    }
  }
  for(i=0; i<nIn; i++) zOut[i] = rot13(zIn[i]);
  zOut[i] = 0;
  sqlite3_result_text(context, (char*)zOut, i, SQLITE_TRANSIENT);
  sqlite3_free(zToFree);
}

int sqlite3_rot_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  rc = sqlite3_create_function(db, "rot13", 1, SQLITE_UTF8, 0,
                               rot13func, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_collation(db, "rot13", SQLITE_UTF8, 0, rot13CollFunc);
  }
  return rc;
}
#endif
/*
int sqlite3_create_function(
  sqlite3 *db,
  const char *zFunctionName,
  int nArg,
  int eTextRep,
  void *pApp,
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**),
  void (*xStep)(sqlite3_context*,int,sqlite3_value**),
  void (*xFinal)(sqlite3_context*)
);
*/

	static void 
gouda_function(
	sqlite3_context						*context,
	int									argc,
	sqlite3_value						**argv
){
	double								a = sqlite3_value_double(argv[0]);
	double								b = sqlite3_value_double(argv[1]);
	double								c = sqlite3_value_double(argv[2]);
	double								d = sqlite3_value_double(argv[3]);
	double								answer;

	answer = a*b - c*d;

	sqlite3_result_double(context, answer);
}

	int 
load_udf_definitions(
	sqlite3								*db,
	const char							**pzErrMsg,
	const struct sqlite3_api_routines	*pApi
){
	int									rc = SQLITE_OK;
	SQLITE_EXTENSION_INIT2(pApi);
	(void)pzErrMsg;						/* Unused parameter */

	rc = sqlite3_create_function(db, "gouda", 4, SQLITE_UTF8|SQLITE_DETERMINISTIC,
									0, gouda_function, 0, 0);

	return(rc);
};

#undef sqlite3_auto_extension

//int sqlite3_auto_extension(void(*xEntryPoint)(void));

//int sqlite3_auto_extension(int xEntryPoint(sqlite3 *db, const char **pzErrMsg, const struct sqlite3_api_routines *pThunk));

//extern "C" void BEDROCK_PLUGIN_REGISTER_UDF() {
void BEDROCK_PLUGIN_REGISTER_UDF() {
//	sqlite3_auto_extension(load_udf_definitions);
	sqlite3_auto_extension((void *) &load_udf_definitions);
}
