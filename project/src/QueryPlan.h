//Class for to store one query plan for enumeration
#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_

#include <map>
#include <vector>
#include <string>
#include <iostream>

#include "DBFile.h"
#include "ParseTree.h"
#include "Pipe.h"
#include "RelOp.h"
#include "QueryPlanNode.h"
#include "Statistics.h"
#include "Record.h"
#include "Defs.h"
#include "Schema.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Function.h"

extern char *catalog_path;
extern char *dbfile_dir;
extern char *tpch_dir;

using namespace std;

class QueryPlan {
private:
	struct FuncOperator * finalFunction; //function in aggregation
	struct TableList * tables;   // Tables in FROM CLAUSE
	struct AndList *cnfAndList;  // AndList from WHER CLAUSE
	struct NameList * groupAtts; // grouping atts (NULL if no grouping)
	struct NameList * attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
	int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
	int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
	Statistics *statistics;

	void GetJoinsAndSelects(vector<AndList*> &joins, vector<AndList*> &selects,
			vector<AndList*> &selAboveJoin);

	map<string, AndList*>* OptimizeSelectAndApply(vector<AndList*> selects);
	vector<AndList*>* OptimizeJoinOrder(vector<AndList*> joins);

	Function *GenerateFunc(Schema *schema);
	OrderMaker *GenerateOM(Schema *schema);

public:
	QueryPlan();
	virtual ~QueryPlan();

	QueryPlanNode *root;
	int pipeNum;
	char* outputType;

	void updateInput(struct FuncOperator *finalFunction,
			struct TableList *tables, struct AndList * boolean,
			struct NameList * pGrpAtts, struct NameList * pAttsToSelect,
			int distinct_atts, int distinct_func, Statistics *);
	void clearQuery();
	int CreatePlan();
	int ExecuteQueryPlan();
	int ExecuteCreateTable(CreateTable*);
	int ExecuteInsertFile(InsertFile*);
	int ExecuteDropTable(char *);
};
#endif /* QUERYPLAN_H_ */
