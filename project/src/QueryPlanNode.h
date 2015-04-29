//Class for to store one query plan for enumeration
#ifndef QUERYPLANNODE_H_
#define QUERYPLANNODE_H_

#include <string.h>
#include <iostream>
#include <map>
#include <vector>

#include "DBFile.h"
#include "Comparison.h"
#include "Pipe.h"
#include "RelOp.h"

using namespace std;

class QueryPlanNode {
public:
	// common members
	int leftInPipeId, outPipeId;
	Schema * outputSchema;
	CNF* cnf;
	static map<int, Pipe*> pipesList;
	static vector<RelationalOp*> relOpList;

	// left and right children (tree structure)
	QueryPlanNode * left;
	QueryPlanNode * right;

	QueryPlanNode() :
			leftInPipeId(-1), outPipeId(-1), left(
			NULL), right(NULL), outputSchema(NULL) {
		cnf = new CNF;
	}

	virtual void PrintNode() = 0;
	void ExecutePostOrder();
	void PrintPostOrder();
	void CreatePipe();
	virtual void ExecuteNode() = 0;
	virtual ~QueryPlanNode() {
	}
};

class SelectPipeQPNode: public QueryPlanNode {
public:
	Record * literal;

	SelectPipeQPNode();
	SelectPipeQPNode(int in, int out, CNF* pCNF, Record * pLit, Schema * pSch);
	~SelectPipeQPNode();
	void PrintNode();
	void ExecuteNode();
};

class SelectFileQPNode: public QueryPlanNode {
public:
	string sFileName;
	Record * literal;

	SelectFileQPNode();
	SelectFileQPNode(string inFile, int out, CNF* pCNF, Record * pLit,
			Schema * pSch);
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
	Record * literal;
	JoinQPNode * parent;

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
	SumQPNode(int ip, int op, Function *pF, bool bPrint, Schema * pSch);
	~SumQPNode();
	void PrintNode();
	void ExecuteNode();
};

class GroupByQPNode: public QueryPlanNode {
public:
	Function * func;
	OrderMaker * orderMaker;

	GroupByQPNode();
	GroupByQPNode(int ip, int op, Function *pF, OrderMaker *pOM, Schema * pSch);
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
#endif /* QUERYPLANNODE_H_ */
