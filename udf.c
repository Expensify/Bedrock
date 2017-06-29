/*
   Copyright 2017 Guy Riddle

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

/*
 * One sample SQLite stored function that is automatically loaded into each database opened.
 *
 * You might use a different cheese, of course.
 *
 * Demonstrate via:		"SELECT gouda(1.2, 2.3, 5.0, 3.4);"
 *
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

	rc = sqlite3_create_function(db, "gouda", 4, SQLITE_UTF8|SQLITE_DETERMINISTIC, 0, gouda_function, 0, 0);

	return(rc);
};

#undef sqlite3_auto_extension

	void
udf_initialize(){
	sqlite3_auto_extension((void *) &load_udf_definitions);
}
