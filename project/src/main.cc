#include <iostream>
#include "Defs.h"
#include "QueryPlan.h"
#include "ParseTree.h"
#include <time.h>

using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

// DDL
extern struct CreateTable *createTable;
extern struct InsertFile *insertFile;
extern char *dropTableName;
extern char *setOutPut;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern int quit;

char *catalog_path;
char *dbfile_dir;
char *tpch_dir;

#ifndef MY_HEADER_
#define MY_HEADER_
inline void clear() {
	createTable = NULL;
	insertFile = NULL;
	dropTableName = NULL;
	setOutPut = NULL;
	finalFunction = NULL;
	tables = NULL;
	boolean = NULL;
	groupingAtts = NULL;
	attsToSelect = NULL;
	distinctAtts = distinctFunc = 0;
}
#endif

int main() {

	catalog_path = "catalog";		// full path of the catalog file
	dbfile_dir = "/Users/abhinavrungta/gitlab/databaseimpl/dbfile/"; // dir where binary heap files should be stored
	tpch_dir = "/Users/abhinavrungta/gitlab/databaseimpl/db/"; // dir where dbgen tpch files (extension *.tbl) can be found

	QueryPlan *queryPlan = new QueryPlan();
	while (true) {
		if (yyparse() != 0) {
			cout << "Can't parse your CNF.\n";
			continue;
		}
		if (quit) {
			cout << "Exited" << endl;
			return 0;
		}

		if (createTable) {
			if (queryPlan->ExecuteCreateTable(createTable)) {
				cout << "Created table" << createTable->tableName << endl;
			}
		} else if (insertFile) {
			if (queryPlan->ExecuteInsertFile(insertFile)) {
				cout << "Loaded file " << insertFile->fileName << " into "
						<< insertFile->tableName << endl;
			}
		} else if (dropTableName) {
			if (queryPlan->ExecuteDropTable(dropTableName)) {
				cout << "Dropped dbfile" << dropTableName << endl;
			}
		} else if (setOutPut) {
			queryPlan->outputType = setOutPut;
		} else if (tables) { // query
			//now we have all the info in the above data structure
			Statistics *s = new Statistics();
			s->LoadAllStatistics();
			queryPlan->updateInput(finalFunction, tables, boolean, groupingAtts,
					attsToSelect, distinctAtts, distinctFunc, s);

			if (queryPlan->CreatePlan() == 0) {
				cerr << "ERROR in building query Plan!" << endl;
				exit(0);
			}

			time_t t1;
			time(&t1);
			queryPlan->ExecuteQueryPlan();
			time_t t2;
			time(&t2);
			cout << "Execution took " << difftime(t2, t1) << " seconds!"
					<< endl;
			queryPlan->clearQuery();
		}
		clear();
	}
	return 0;
}
