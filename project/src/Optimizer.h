#ifndef OPTIMIZER_H_
#define OPTIMIZER_H_

#include <map>
#include <vector>
#include <string>
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryPlan.h"
#include "Record.h"
#include "Defs.h"
#include <stdio.h>
#include "Schema.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Function.h"

class Optimizer {
private:
	struct FuncOperator * finalFunction; //function in aggregation
	struct TableList * tables;   // Tables in FROM CLAUSE
	struct AndList *cnfAndList;  // AndList from WHER CLAUSE
	struct NameList * groupAtts; // grouping atts (NULL if no grouping)
	struct NameList * attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
	int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
	int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
	Statistics *statistics;

public:
	Optimizer();
	Optimizer(struct FuncOperator *finalFunction, struct TableList *tables,
			struct AndList * boolean, struct NameList * pGrpAtts,
			struct NameList * pAttsToSelect, int distinct_atts,
			int distinct_func, Statistics *);

	QueryPlan * OptimizedQueryPlan();
	virtual ~Optimizer();
};
#endif /* OPTIMIZER_H_ */
