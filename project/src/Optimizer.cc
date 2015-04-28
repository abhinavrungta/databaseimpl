#include "Optimizer.h"

extern char *catalog_path;
extern char *dbfile_dir;
extern char *tpch_dir;

Optimizer::Optimizer() {
}

Optimizer::~Optimizer() {
}

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

void Optimizer::GetJoinsAndSelects(vector<AndList*> &joins,
		vector<AndList*> &selects, vector<AndList*> &selAboveJoin) {
	OrList *aOrList = NULL;
	AndList *aAndList = this->cnfAndList;
	while (aAndList != NULL) {
		aOrList = aAndList->left;
		if (aOrList == NULL) {
			cerr << "Error in cnf AndList" << endl;
			return;
		}
		if (aOrList->left->code == EQUALS && aOrList->left->left->code == NAME
				&& aOrList->left->right->code == NAME) { //A.a = B.b
			AndList *newAnd = new AndList();
			newAnd->left = aOrList;
			newAnd->rightAnd = NULL;
			joins.push_back(newAnd);
		} else {
			if (aOrList->rightOr == NULL) {  // A.a = 3 like
				AndList *newAnd = new AndList();
				newAnd->left = aOrList;
				newAnd->rightAnd = NULL;
				selects.push_back(newAnd);
			} else {  // like A.a=3 OR A.b<10 ,  A.a=3 OR B.b >10, etc
				vector<string> involvedTables;
				OrList *olp = aOrList;
				while (aOrList != NULL) {
					// Get operand with attribute Name.
					Operand *op = aOrList->left->left;
					if (op->code != NAME) {
						op = aOrList->left->right;
					}
					// Get Relation Name for Attribute
					string rel;
					string opVal = string(op->value);
					if (this->statistics->GetRelation(opVal, rel) == 0) {
						cerr << "Error in parse relations" << endl;
						return;
					}
					if (involvedTables.size() == 0) {
						involvedTables.push_back(rel);
					} else if (rel.compare(involvedTables.at(0)) != 0) {
						involvedTables.push_back(rel);
					}

					aOrList = aOrList->rightOr;
				}

				if (involvedTables.size() > 1) {
					AndList *newAnd = new AndList();
					newAnd->left = olp;
					newAnd->rightAnd = NULL;
					selAboveJoin.push_back(newAnd);
				} else {
					AndList *newAnd = new AndList();
					newAnd->left = olp;
					newAnd->rightAnd = NULL;
					selects.push_back(newAnd);
				}
			}
		}
		aAndList = aAndList->rightAnd;
	}
}

map<string, AndList*>* Optimizer::OptimizeSelectAndApply(
		vector<AndList*> selects) {
	map<string, AndList*> *selectors = new map<string, AndList*>;
	for (vector<AndList*>::iterator it = selects.begin(); it != selects.end();
			it++) {
		AndList *aAndList = *it;
		Operand *op = aAndList->left->left->left;
		if (op->code != NAME)
			op = aAndList->left->left->right;
		string rel;
		string opVal = string(op->value);
		this->statistics->GetRelation(opVal, rel);

		//add it to the select operator
		map<string, AndList*>::iterator mit;
		for (mit = selectors->begin(); mit != selectors->end(); mit++) {
			if (mit->first.compare(rel) == 0) {
				AndList *lastAnd = mit->second;
				while (lastAnd->rightAnd != NULL)
					lastAnd = lastAnd->rightAnd;
				lastAnd->rightAnd = aAndList;
				break;
			}
		}
		if (mit == selectors->end()) //new selector
			selectors->insert(pair<string, AndList*>(rel, aAndList));
	}
	return selectors;
}

vector<AndList*>* Optimizer::OptimizeJoinOrder(vector<AndList*> joins) {
	//A greedy algorithm to pick the sort order
	vector<AndList*> *orderedAndList = new vector<AndList*>;
	orderedAndList->reserve(joins.size());
	if (joins.size() <= 1) {
		if (joins.size() == 1)
			orderedAndList->push_back(joins[0]);
	} else {
		int size = joins.size();
		for (int i = 0; i < size; i++) {
			int joinNum = 2;
			double smallest = 0.0;
			string left_rel, right_rel;
			AndList *chooseAndList;
			int choosePos = -1;
			for (int j = 0; j < joins.size(); j++) {
				string rel1, rel2;
				AndList *andList = joins[j];
				Operand *l = andList->left->left->left;
				Operand *r = andList->left->left->right;
				string lVal = l->value;
				string rVal = r->value;
				this->statistics->GetRelation(lVal, rel1);
				this->statistics->GetRelation(rVal, rel2);

				char *estrels[] = { (char*) rel1.c_str(), (char*) rel2.c_str() };
				double cost = this->statistics->Estimate(andList, estrels, 2);
				if (choosePos == -1 || cost < smallest) {
					smallest = cost;
					left_rel = rel1;
					right_rel = rel2;
					chooseAndList = andList;
					choosePos = j;
				}
			}
			//find the andList
			orderedAndList->push_back(chooseAndList);
			char *aplyrels[] = { (char*) left_rel.c_str(),
					(char*) right_rel.c_str() };
			this->statistics->Apply(chooseAndList, aplyrels, 2);

			joins.erase(joins.begin() + choosePos);
		}
		return orderedAndList;
	}
}

void PrintOperand(struct Operand *pOperand) {
	if (pOperand != NULL) {
		cout << pOperand->value << " ";
	} else
		return;
}

void PrintComparisonOp(struct ComparisonOp *pCom) {
	if (pCom != NULL) {
		PrintOperand(pCom->left);
		switch (pCom->code) {
		case LESS_THAN:
			cout << " < ";
			break;
		case GREATER_THAN:
			cout << " > ";
			break;
		case EQUALS:
			cout << " = ";
		}
		PrintOperand(pCom->right);

	} else {
		return;
	}
}
void PrintOrList(struct OrList *pOr) {
	if (pOr != NULL) {
		struct ComparisonOp *pCom = pOr->left;
		PrintComparisonOp(pCom);

		if (pOr->rightOr != NULL) {
			cout << " OR ";
			PrintOrList(pOr->rightOr);
		}
	} else {
		return;
	}
}
void PrintAndList(struct AndList *pAnd) {
	if (pAnd != NULL) {
		struct OrList *pOr = pAnd->left;
		PrintOrList(pOr);
		if (pAnd->rightAnd != NULL) {
			cout << " AND ";
			PrintAndList(pAnd->rightAnd);
		}
	} else {
		return;
	}
}

void printStringVector(vector<string> v) {
	for (vector<string>::iterator it = v.begin(); it != v.end(); it++) {
		cout << *it << ", ";
	}
	cout << endl;
}
void printAndListVector(vector<AndList*> *v) {
	for (vector<AndList*>::iterator it = v->begin(); it != v->end(); it++) {
		PrintAndList(*it);
		cout << ";  ";
	}
	cout << endl;
}
//---------------------------print out functions end-------------
OrderMaker *Optimizer::GenerateOM(Schema *schema) {
	OrderMaker *order = new OrderMaker();
	NameList *name = this->groupAtts;
	while (name) {
		order->whichAtts[order->numAtts] = schema->Find(name->name);
		order->whichTypes[order->numAtts] = schema->FindType(name->name);
		order->numAtts++;
		name = name->next;
	}
	return order;
}
Function *Optimizer::GenerateFunc(Schema *schema) {
	Function *func = new Function();
	func->GrowFromParseTree(this->finalFunction, *schema);
	return func;
}

//-----------------------------------------------------build up the query plan---------
QueryPlan * Optimizer::OptimizedQueryPlan() {
	// Copy Relations for all Tables with Aliases.
	TableList *list = tables;
	while (list) {
		if (list->aliasAs) {
			this->statistics->CopyRel(list->tableName, list->aliasAs);
		}
		list = list->next;
	}

	vector<AndList*> joins;
	vector<AndList*> selects;
	vector<AndList*> selAboveJoin;
	GetJoinsAndSelects(joins, selects, selAboveJoin);
	map<string, AndList*>* selectors = this->OptimizeSelectAndApply(selects);
	vector<AndList*>* orderedJoins = this->OptimizeJoinOrder(joins);

	//-------now build the query plan ------
	QueryPlan *queryPlan = new QueryPlan();
	// first build the select from file
	map<string, SelectFileQPNode *>* selectFromFiles = new map<string,
			SelectFileQPNode*>(); //store the selector
	for (TableList *table = this->tables; table != NULL; table = table->next) {
		// Get schema for the relation Names.
		char name[100];
		sprintf(name, "%s%s.bin", dbfile_dir, table->tableName);
		Schema *sch = new Schema(catalog_path, table->tableName);

		SelectFileQPNode *selectFile = new SelectFileQPNode();
		selectFile->sFileName = string(name);
		selectFile->outPipeId = queryPlan->pipeNum++;
		selectFile->CreatePipe();

		// Build Schema from original schema for relations with alias.
		string aliasRelName(table->tableName);
		if (table->aliasAs != NULL) { //supp AS s
			sch->UpdateSchemaForAlias(table->aliasAs);
			aliasRelName = string(table->aliasAs);
		}

		selectFile->outputSchema = sch;
		selectFile->literal = new Record;

		// iterate thru the AndLists for given alias.
		map<string, AndList*>::iterator it;
		for (it = selectors->begin(); it != selectors->end(); it++) {
			if (aliasRelName.compare(it->first) == 0)
				break;
		}
		AndList *andList;
		if (it == selectors->end())
			andList = NULL;
		else
			andList = it->second;

		// Get the CNF from the andList for given relation.
		selectFile->cnf->GrowFromParseTree(andList, selectFile->outputSchema,
				*(selectFile->literal));

		// add to the map.
		selectFromFiles->insert(
				pair<string, SelectFileQPNode*>(aliasRelName, selectFile));
	}

	//------------then build the joins
	map<string, JoinQPNode *>* builtJoins = new map<string, JoinQPNode*>();
	JoinQPNode *join = NULL;

	if (orderedJoins->size() > 0) {
		for (vector<AndList*>::iterator jIt = orderedJoins->begin();
				jIt != orderedJoins->end(); jIt++) {
			AndList *aAndList = *jIt;

			// Get the relation names of the join condition.
			Operand *leftAtt = aAndList->left->left->left;
			string leftRel;
			string lefAttVal = string(leftAtt->value);
			this->statistics->GetRelation(lefAttVal, leftRel);
			Operand *rightAtt = aAndList->left->left->right;
			string rightRel;
			string rightAttVal = string(rightAtt->value);
			this->statistics->GetRelation(rightAttVal, rightRel);

			join = new JoinQPNode();

			// Check if the join has already been done for the relation. i.e. it was already a part of a join.
			JoinQPNode *leftUpMost = NULL;
			JoinQPNode *rightUpMost = NULL;

			try {
				leftUpMost = (*builtJoins).at(leftRel);
			} catch (exception e) {
			}

			try {
				rightUpMost = (*builtJoins).at(rightRel);
			} catch (exception e1) {
			}

			if (leftUpMost == NULL && rightUpMost == NULL) { // !A and !B
				join->left = (*selectFromFiles)[leftRel];
				join->right = (*selectFromFiles)[rightRel];
			} else if (leftUpMost != NULL) { //A and !B
				while (leftUpMost->parent)
					leftUpMost = leftUpMost->parent;
				join->left = leftUpMost;
				leftUpMost->parent = join;
				join->right = (*selectFromFiles)[rightRel];
			} else if (rightUpMost != NULL) { //!A and B
				while (rightUpMost->parent)
					rightUpMost = rightUpMost->parent;
				join->left = rightUpMost;
				rightUpMost->parent = join;
				join->right = (*selectFromFiles)[leftRel];
			} else { // A and B
				while (leftUpMost->parent)
					leftUpMost = leftUpMost->parent;
				while (rightUpMost->parent)
					rightUpMost = rightUpMost->parent;
				join->left = leftUpMost;
				leftUpMost->parent = join;
				join->right = rightUpMost;
				rightUpMost->parent = join;
			}
			// Set Variables.
			join->leftInPipeId = join->left->outPipeId;
			join->rightInPipeId = join->right->outPipeId;
			join->outPipeId = queryPlan->pipeNum++;
			join->CreatePipe();
			join->outputSchema = new Schema(join->left->outputSchema,
					join->right->outputSchema);
			join->literal = new Record();
			join->cnf->GrowFromParseTree(aAndList, join->left->outputSchema,
					join->right->outputSchema, *(join->literal));

			builtJoins->insert(pair<string, JoinQPNode*>(leftRel, join));
			builtJoins->insert(pair<string, JoinQPNode*>(rightRel, join));
		}
	}

	// the selection above join
	SelectPipeQPNode *selAbvJoin = NULL;
	if (selAboveJoin.size() > 0) {
		selAbvJoin = new SelectPipeQPNode();
		if (join == NULL) {
			selAbvJoin->left = selectFromFiles->begin()->second;
		} else {
			selAbvJoin->left = join;
		}
		selAbvJoin->leftInPipeId = selAbvJoin->left->outPipeId;
		selAbvJoin->outPipeId = queryPlan->pipeNum++;
		selAbvJoin->outputSchema = selAbvJoin->left->outputSchema;
		selAbvJoin->CreatePipe();
		AndList *andList = *(selAboveJoin.begin());
		for (vector<AndList*>::iterator it = selAboveJoin.begin();
				it != selAboveJoin.end(); it++) {
			if (it != selAboveJoin.begin()) {
				andList->rightAnd = *it;
			}
		}
		selAbvJoin->literal = new Record();
		selAbvJoin->cnf->GrowFromParseTree(andList, selAbvJoin->outputSchema,
				*(selAbvJoin)->literal);
	}

	//build group by if any
	GroupByQPNode *groupBy = NULL;
	if (this->groupAtts != NULL) {
		groupBy = new GroupByQPNode();
		if (selAbvJoin != NULL) {
			groupBy->left = selAbvJoin;
		} else if (join != NULL) {
			groupBy->left = join;
		} else {
			groupBy->left = selectFromFiles->begin()->second;
		}
		groupBy->leftInPipeId = groupBy->left->outPipeId;
		groupBy->outPipeId = queryPlan->pipeNum++;
		groupBy->CreatePipe();
		groupBy->orderMaker = this->GenerateOM(groupBy->left->outputSchema);
		groupBy->func = this->GenerateFunc(groupBy->left->outputSchema);

		Attribute *attr = new Attribute[1];
		attr[0].name = (char *) "sum";
		attr[0].myType = Double;
		Schema *sumSchema = new Schema((char *) "dummy", 1, attr);

		NameList *attName = this->groupAtts;
		int numGroupAttrs = 0;
		while (attName) {
			numGroupAttrs++;
			attName = attName->next;
		}

		if (numGroupAttrs == 0) {
			groupBy->outputSchema = sumSchema;
		} else {
			Attribute *attrs = new Attribute[numGroupAttrs];
			int i = 0;
			attName = this->groupAtts;
			while (attName) {
				attrs[i].name = strdup(attName->name);
				attrs[i++].myType = groupBy->left->outputSchema->FindType(
						attName->name);
				attName = attName->next;
			}
			Schema *outSchema = new Schema((char *) "dummy", numGroupAttrs,
					attrs);
			groupBy->outputSchema = new Schema(sumSchema, outSchema);
		}
	}

	//------build the SUM if any------
	SumQPNode *sum = NULL;
	if (groupBy == NULL && this->finalFunction != NULL) {
		sum = new SumQPNode;
		if (selAbvJoin != NULL)
			sum->left = selAbvJoin;
		else if (join != NULL)
			sum->left = join;
		else
			sum->left = selectFromFiles->begin()->second;
		sum->leftInPipeId = sum->left->outPipeId;
		sum->outPipeId = queryPlan->pipeNum++;
		sum->func = this->GenerateFunc(sum->left->outputSchema);
		sum->CreatePipe();

		Attribute *attr = new Attribute[1];
		attr[0].name = (char *) "sum";
		attr[0].myType = Double;
		sum->outputSchema = new Schema((char *) "dummy", 1, attr);
	}

	//-------build the project --------
	ProjectQPNode *project = new ProjectQPNode();
	int outputNum = 0;
	NameList *name = this->attsToSelect;
	Attribute *outputAtts;
	while (name) {  // Getting the output Attr num
		outputNum++;
		name = name->next;
	}
	int ithAttr = 0;
	if (groupBy != NULL) {
		project->left = groupBy;
		outputNum++;
		project->attributeList = new int[outputNum];
		project->attributeList[0] = groupBy->outputSchema->Find((char *) "sum");
		outputAtts = new Attribute[outputNum + 1];
		outputAtts[0].name = (char *) "sum";
		outputAtts[0].myType = Double;
		ithAttr = 1;
	} else if (sum != NULL) { // we have SUM
		project->left = sum;
		outputNum++;
		project->attributeList = new int[outputNum];
		project->attributeList[0] = sum->outputSchema->Find((char *) "sum");
		outputAtts = new Attribute[outputNum];
		outputAtts[0].name = (char*) "sum";
		outputAtts[0].myType = Double;
		ithAttr = 1;
	} else if (join != NULL) {
		project->left = join;
		if (outputNum == 0) {
			cerr << "No attributes assigned to select!" << endl;
			return NULL;
		}
		project->attributeList = new int[outputNum];
		outputAtts = new Attribute[outputNum];
	} else {
		project->left = selectFromFiles->begin()->second;
		if (outputNum == 0) {
			cerr << "No attributes assigned to select!" << endl;
			return NULL;
		}
		project->attributeList = new int[outputNum];
		outputAtts = new Attribute[outputNum];
	}
	name = this->attsToSelect;
	while (name) {
		project->attributeList[ithAttr] = project->left->outputSchema->Find(
				name->name);
		outputAtts[ithAttr].name = name->name;
		outputAtts[ithAttr].myType = project->left->outputSchema->FindType(
				name->name);
		ithAttr++;
		name = name->next;
	}
	project->iTotalAttributes = project->left->outputSchema->GetNumAtts();
	project->iAtttributesToKeep = outputNum;
	project->leftInPipeId = project->left->outPipeId;
	project->outPipeId = queryPlan->pipeNum++;
	project->outputSchema = new Schema((char*) "dummy", outputNum, outputAtts);
	project->CreatePipe();

	queryPlan->root = project;

	//-------build the distinct -------
	DistinctQPNode *distinct = NULL;
	if (this->distinctAtts) { //we have distinct
		distinct = new DistinctQPNode;
		distinct->left = project;
		distinct->leftInPipeId = distinct->left->outPipeId;
		distinct->outputSchema = distinct->left->outputSchema;
		distinct->outPipeId = queryPlan->pipeNum++;
		distinct->CreatePipe();
		queryPlan->root = distinct;
	}
	return queryPlan;
}
