//Class for to store one query plan for enumeration
#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_

#include "RelOp.h"
#include "Pipe.h"
#include <map>
#include <string.h>
#include "ParseTree.h"
#include "DBFile.h"
#include <iostream>
#include <fstream>
#include <vector>

using namespace std;
#define QUERY_PIPE_SIZE 100
#define QUERY_USE_PAGES 100

class QueryPlanNode {
public:
	// common members
	int leftInPipeId, outPipeId;
	Schema * outputSchema;
	static map<int, Pipe*> pipesList;

	// left and right children (tree structure)
	QueryPlanNode * left;
	QueryPlanNode * right;
	QueryPlanNode * parent;

	QueryPlanNode() :
			leftInPipeId(-1), outPipeId(-1), left(
			NULL), right(NULL), outputSchema(NULL), parent(NULL) {
	}

	virtual void PrintNode() = 0;
	void ExecutePostOrder();
	virtual void ExecuteNode() = 0;
	virtual ~QueryPlanNode() {
	}
};

class SelectPipeQPNode: public QueryPlanNode {
public:
	CNF* cnf;
	Record * literal;

	SelectPipeQPNode();
	SelectPipeQPNode(int in, int out, CNF* pCNF, Record * pLit);
	~SelectPipeQPNode();
	void PrintNode();
	void ExecuteNode();
};

class SelectFileQPNode: public QueryPlanNode {
public:
	string sFileName;
	CNF* cnf;
	Record * literal;

	SelectFileQPNode();
	SelectFileQPNode(string inFile, int out, CNF* pCNF, Record * pLit);
	~SelectFileQPNode();
	void PrintNode();
	void ExecuteNode();
};

class ProjectQPNode: public QueryPlanNode {
public:
	int * attributeList;
	int iAtttributesToKeep, iTotalAttributes;

	ProjectQPNode();
	ProjectQPNode(int ip, int op, int *atk, int nKeep, int nTot, Schema * pSch);
	~ProjectQPNode();
	void PrintNode();
	void ExecuteNode();
};

class JoinQPNode: public QueryPlanNode {
public:
	int rightInPipeId;
	CNF* cnf;
	Record * literal;

	JoinQPNode();
	JoinQPNode(int ip1, int ip2, int op, CNF* pCNF, Schema * pSch,
			Record * pLit);
	~JoinQPNode();
	void PrintNode();
	void ExecuteNode();
};

class SumQPNode: public QueryPlanNode {
public:
	Function * func;

	SumQPNode();
	SumQPNode(int ip, int op, Function *pF, bool bPrint);
	~SumQPNode();
	void PrintNode();
	void ExecuteNode();
};

class GroupByQPNode: public QueryPlanNode {
public:
	Function * func;
	OrderMaker * orderMaker;

	GroupByQPNode();
	GroupByQPNode(int ip, int op, Function *pF, OrderMaker *pOM);
	~GroupByQPNode();
	void PrintNode();
	void ExecuteNode();
};

class DistinctQPNode: public QueryPlanNode {
public:
	DistinctQPNode();
	DistinctQPNode(int ip, int op, Schema * pSch);
	~DistinctQPNode();
	void PrintNode();
	void ExecuteNode();
};

class WriteOutQPNode: public QueryPlanNode {
public:
	string outFileName;

	WriteOutQPNode();
	WriteOutQPNode(int ip, string outFile, Schema * pSch);
	~WriteOutQPNode();
	void PrintNode();
	void ExecuteNode();
};

class QueryPlan {
	char* catalog_path;
	char* dbfile_dir;
	char* tpch_dir;
public:
	QueryPlan();
	QueryPlan(char* catalog_path, char* dbfile_dir, char* tpch_dir);
	virtual ~QueryPlan();

	QueryPlanNode *root;
	int pipeNum;
	char* outputType;

	void PrintInOrder();
	int ExecuteQueryPlan();
	int ExecuteCreateTable(CreateTable*);
	int ExecuteInsertFile(InsertFile*);
	int ExecuteDropTable(char *);
};
#endif /* QUERYPLAN_H_ */
