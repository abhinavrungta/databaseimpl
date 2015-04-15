#include "Optimizer.h"

Optimizer::Optimizer(struct FuncOperator *finalFunction,
		struct TableList *tables, struct AndList * boolean,
		struct NameList * pGrpAtts, struct NameList * pAttsToSelect,
		int distinct_atts, int distinct_func, Statistics *s) {
	this->finalFunction = finalFunction;
	this->tables = tables;
	this->cnfAndList = boolean;
	this->groupAtts = pGrpAtts;
	this->attsToSelect = pAttsToSelect;
	this->distinctAtts = distinct_atts;
	this->distinctFunc = distinct_func;
	this->statistics = s;
}

QueryPlan* Optimizer::OptimizedQueryPlan() {
}
