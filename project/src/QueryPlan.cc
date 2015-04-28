#include "QueryPlan.h"

using namespace std;
extern char *catalog_path;
extern char *dbfile_dir;
extern char *tpch_dir;

map<int, Pipe*> QueryPlanNode::pipesList;
vector<RelationalOp*> QueryPlanNode::relOpList;

void QueryPlanNode::ExecutePostOrder() {
	if (this->left)
		this->left->ExecutePostOrder();
	if (this->right)
		this->right->ExecutePostOrder();
	cout << "Pipe id" << this->outPipeId << endl;
	this->ExecuteNode();
}

void QueryPlanNode::PrintPostOrder() {
	if (this->left)
		this->left->PrintPostOrder();
	if (this->right)
		this->right->PrintPostOrder();
	this->PrintNode();
}

void QueryPlanNode::CreatePipe() {
	pipesList[outPipeId] = new Pipe(PIPE_SIZE);
}

// -------------------------------------- select pipe ------------------
SelectPipeQPNode::SelectPipeQPNode() {
}

SelectPipeQPNode::SelectPipeQPNode(int in, int out, CNF* pCNF, Record * pLit,
		Schema * pSch) {
	leftInPipeId = in;
	outPipeId = out;
	cnf = pCNF;
	literal = pLit;
	outputSchema = pSch;
	pipesList[outPipeId] = new Pipe(PIPE_SIZE);
}

SelectPipeQPNode::~SelectPipeQPNode() {
	if (this->left)
		delete this->left;
	if (this->right)
		delete this->right;

	if (cnf != NULL) {
		delete cnf;
		cnf = NULL;
	}
	if (literal != NULL) {
		delete literal;
		literal = NULL;
	}
}

void SelectPipeQPNode::PrintNode() {
	cout << "\n*** Select Pipe Operation ***";
	cout << "\nInput pipe ID: " << leftInPipeId;
	cout << "\nOutput pipe ID: " << outPipeId;

	cout << "\nOutput Schema: ";
	if (outputSchema != NULL)
		outputSchema->Print();
	else
		cout << "NULL";

	cout << "\nSelect CNF : ";
	if (cnf != NULL)
		cnf->Print();
	else
		cout << "NULL";
	cout << endl << endl;
}

void SelectPipeQPNode::ExecuteNode() {
	SelectPipe *selPipe = new SelectPipe();
	if (cnf != NULL && literal != NULL) {
		selPipe->Run(*(pipesList[leftInPipeId]), *(pipesList[outPipeId]), *cnf,
				*literal);
		relOpList.push_back(selPipe);
	}
}

// -------------------------------------- select file ------------------
SelectFileQPNode::SelectFileQPNode() {
}

SelectFileQPNode::SelectFileQPNode(string inFile, int out, CNF* pCNF,
		Record * pLit, Schema *pScH) {
	sFileName = inFile;
	outPipeId = out;
	cnf = pCNF;
	literal = pLit;
	outputSchema = pScH;
	pipesList[outPipeId] = new Pipe(PIPE_SIZE);
}

SelectFileQPNode::~SelectFileQPNode() {
	if (this->left != NULL)
		delete this->left;
	if (this->right != NULL)
		delete this->right;

	if (cnf != NULL) {
		delete cnf;
		cnf = NULL;
	}
	if (literal != NULL) {
		delete literal;
		literal = NULL;
	}
}

void SelectFileQPNode::PrintNode() {

	cout << "\n*** Select File Operation ***";
	cout << "\nOutput pipe ID: " << outPipeId;
	cout << "\nInput filename: " << sFileName.c_str();

	cout << "\nOutput Schema: ";
	if (outputSchema != NULL)
		outputSchema->Print();
	else
		cout << "NULL";

	cout << "\nSelect CNF : ";
	if (cnf != NULL)
		cnf->Print();
	else
		cout << "NULL";
	cout << endl << endl;
}

void SelectFileQPNode::ExecuteNode() {
	//create a DBFile from input file path provided
	DBFile * pFile = new DBFile;
	pFile->Open(const_cast<char*>(sFileName.c_str()));

	SelectFile * pSF = new SelectFile();
	if (cnf != NULL && literal != NULL) {
		pSF->Run(*pFile, *(pipesList[outPipeId]), *cnf, *literal);
		relOpList.push_back(pSF);
	}
}

// -------------------------------------- project ------------------
ProjectQPNode::ProjectQPNode() {

}

ProjectQPNode::ProjectQPNode(int ip, int op, int *atk, int nKeep, int nTot,
		Schema * pSch) {
	leftInPipeId = ip;
	outPipeId = op;
	attributeList = atk;
	iAtttributesToKeep = nKeep;
	iTotalAttributes = nTot;
	outputSchema = pSch;
	pipesList[outPipeId] = new Pipe(PIPE_SIZE);
}

ProjectQPNode::~ProjectQPNode() {
	if (this->left)
		delete this->left;
	if (this->right)
		delete this->right;

	if (attributeList) {
		delete[] attributeList;
		attributeList = NULL;
	}
}

void ProjectQPNode::PrintNode() {
	cout << "\n*** Project Node Operation ***";
	cout << "\nInput pipe ID: " << leftInPipeId;
	cout << "\nOutput pipe ID: " << outPipeId;
	cout << "\nNum atts to Keep: " << iAtttributesToKeep;
	cout << "\nNum total atts: " << iTotalAttributes;
	if (attributeList != NULL) {
		cout << "\nAttributes to keep: ";
		for (int i = 0; i < iAtttributesToKeep; i++)
			cout << attributeList[i] << "  ";
	}
	cout << "\nOutput Schema: ";
	if (outputSchema != NULL)
		outputSchema->Print();
	else
		cout << "NULL";

	cout << endl << endl;
}

void ProjectQPNode::ExecuteNode() {
	Project *P = new Project();
	if (attributeList != NULL) {
		P->Run(*(pipesList[leftInPipeId]), *(pipesList[outPipeId]),
				attributeList, iTotalAttributes, iAtttributesToKeep);
		relOpList.push_back(P);
	}
}

// -------------------------------------- join ------------------
JoinQPNode::JoinQPNode() {
}

JoinQPNode::JoinQPNode(int ip1, int ip2, int op, CNF* pCNF, Schema * pSch,
		Record * pLit) {
	leftInPipeId = ip1;
	rightInPipeId = ip2;
	outPipeId = op;
	cnf = pCNF;
	outputSchema = pSch;
	literal = pLit;
	pipesList[outPipeId] = new Pipe(PIPE_SIZE);
}

JoinQPNode::~JoinQPNode() {
}

void JoinQPNode::PrintNode() {
	cout << "\n*** Join Operation ***";
	cout << "\nInput pipe-1 ID: " << leftInPipeId;
	cout << "\nInput pipe-2 ID: " << rightInPipeId;
	cout << "\nOutput pipe ID: " << outPipeId;

	cout << "\nOutput Schema: ";
	if (outputSchema != NULL)
		outputSchema->Print();
	else
		cout << "NULL";

	cout << "\nSelect CNF : ";
	if (cnf != NULL)
		cnf->Print();
	else
		cout << "NULL";
	cout << endl << endl;
}

void JoinQPNode::ExecuteNode() {
	Join *J = new Join();
	J->Use_n_Pages(USE_PAGES);
	if (cnf != NULL && literal != NULL) {
		J->Run(*(pipesList[leftInPipeId]), *(pipesList[rightInPipeId]),
				*(pipesList[outPipeId]), *cnf, *literal);
		relOpList.push_back(J);
	}
}

// -------------------------------------- group by  ------------------
GroupByQPNode::GroupByQPNode() {
}

GroupByQPNode::GroupByQPNode(int ip, int op, Function *pF, OrderMaker *pOM,
		Schema *pSch) {
	leftInPipeId = ip;
	outPipeId = op;
	func = pF;
	orderMaker = pOM;
	outputSchema = pSch;
	pipesList[outPipeId] = new Pipe(PIPE_SIZE);
}

GroupByQPNode::~GroupByQPNode() {
	if (this->left)
		delete this->left;
	if (this->right)
		delete this->right;

	if (func) {
		delete func;
		func = NULL;
	}
	if (orderMaker) {
		delete orderMaker;
		orderMaker = NULL;
	}
}

void GroupByQPNode::PrintNode() {
	cout << "\n*** Group-by Operation ***";
	cout << "\nInput pipe ID: " << leftInPipeId;
	cout << "\nOutput pipe ID: " << outPipeId;
	cout << "\nFunction: ";
	if (func)
		func->Print();
	else
		cout << "NULL\n";
	cout << "\nOrderMaker:\n";
	if (orderMaker)
		orderMaker->Print();
	else
		cout << "NULL\n";
	cout << "\nOutput Schema: ";
	if (outputSchema != NULL)
		outputSchema->Print();
	else
		cout << "NULL";

	cout << endl << endl;
}

void GroupByQPNode::ExecuteNode() {
	GroupBy *G = new GroupBy();
	if (func != NULL && orderMaker != NULL) {
		G->Run(*(pipesList[leftInPipeId]), *(pipesList[outPipeId]), *orderMaker,
				*func);
		relOpList.push_back(G);
	}
}

// -------------------------------------- sum ------------------
SumQPNode::SumQPNode() {
}

SumQPNode::SumQPNode(int ip, int op, Function *pF, bool bPrint, Schema *pSch) {
	leftInPipeId = ip;
	outPipeId = op;
	func = pF;
	outputSchema = pSch;
	pipesList[outPipeId] = new Pipe(PIPE_SIZE);
}

SumQPNode::~SumQPNode() {
	if (this->left)
		delete this->left;
	if (this->right)
		delete this->right;

	if (func) {
		delete func;
		func = NULL;
	}
}

void SumQPNode::PrintNode() {
	cout << "\n*** Sum Operation ***";
	cout << "\nInput pipe ID: " << leftInPipeId;
	cout << "\nOutput pipe ID: " << outPipeId;
	cout << "\nFunction: ";
	func->Print();
	cout << "\nOutput Schema: ";
	if (outputSchema != NULL)
		outputSchema->Print();
	else
		cout << "NULL";

	cout << endl << endl;
}

void SumQPNode::ExecuteNode() {
	Sum *S = new Sum();
	if (func != NULL) {
		S->Run(*(pipesList[leftInPipeId]), *(pipesList[outPipeId]), *func);
		relOpList.push_back(S);
	}
}

// -------------------------------------- Distinct ------------------
DistinctQPNode::DistinctQPNode() {
}

DistinctQPNode::DistinctQPNode(int ip, int op, Schema * pSch) {
	leftInPipeId = ip;
	outPipeId = op;
	outputSchema = pSch;
	pipesList[outPipeId] = new Pipe(PIPE_SIZE);
}

DistinctQPNode::~DistinctQPNode() {
	if (this->left)
		delete this->left;
	if (this->right)
		delete this->right;

	if (outputSchema) {
		delete outputSchema;
		outputSchema = NULL;
	}
}

void DistinctQPNode::PrintNode() {
	cout << "\n*** Distinct Operation ***";
	cout << "\nInput pipe ID: " << leftInPipeId;
	cout << "\nOutput pipe ID: " << outPipeId;
	cout << "\nOutput Schema: ";
	if (outputSchema != NULL)
		outputSchema->Print();
	else
		cout << "NULL";

	cout << endl << endl;
}

void DistinctQPNode::ExecuteNode() {
	DuplicateRemoval *DR = new DuplicateRemoval();
	if (outputSchema != NULL) {
		DR->Run(*(pipesList[leftInPipeId]), *(pipesList[outPipeId]),
				*outputSchema);
		relOpList.push_back(DR);
	}
}

// -------------------------------------- write out ------------------
WriteOutQPNode::WriteOutQPNode() {
}

WriteOutQPNode::WriteOutQPNode(int ip, string outFile, Schema * pSch) {
	leftInPipeId = ip;
	outFileName = outFile;
	outputSchema = pSch;
}

WriteOutQPNode::~WriteOutQPNode() {
	if (this->left)
		delete this->left;
	if (this->right)
		delete this->right;

	if (outputSchema) {
		delete outputSchema;
		outputSchema = NULL;
	}
}

void WriteOutQPNode::PrintNode() {
	cout << "\n*** WriteOut Operation ***";
	cout << "\nInput pipe ID: " << leftInPipeId;
	cout << "\nOutput file: " << outFileName;
	cout << "\nOutput Schema: ";
	if (outputSchema != NULL)
		outputSchema->Print();
	else
		cout << "NULL";

	cout << endl << endl;
}

void WriteOutQPNode::ExecuteNode() {
	WriteOut *W = new WriteOut();
	if (outputSchema != NULL && !outFileName.empty()) {
		FILE * pFILE;
		if (strcmp(outFileName.c_str(), "STDOUT") == 0) {
			pFILE = stdout;
		} else {
			pFILE = fopen((char*) outFileName.c_str(), "w");
		}
		W->Run(*(pipesList[leftInPipeId]), pFILE, *outputSchema);
		relOpList.push_back(W);
	}
}

QueryPlan::QueryPlan() {
	this->pipeNum = 0;
	this->outputType = "STDOUT";
}

QueryPlan::~QueryPlan() {
}

// ------------------Execute ------------
int QueryPlan::ExecuteCreateTable(CreateTable *createTable) {
	DBFile *db = new DBFile;
	char dbpath[100];
	sprintf(dbpath, "%s%s.bin", dbfile_dir, createTable->tableName);
	struct SortInfo {
		OrderMaker *myOrder;
		int runLength;
	};
	SortInfo *info = new SortInfo();
	OrderMaker *om = new OrderMaker;
	if (createTable->type == SORTED) {
		NameList *sortAtt = createTable->sortAttrList;
		while (sortAtt) {
			AttrList *atts = createTable->attrList;
			int i = 0;
			while (atts) {
				if (strcmp(sortAtt->name, atts->attr->attrName)) {
					//got it
					om->whichAtts[om->numAtts] = i;
					om->whichTypes[om->numAtts] = (Type) atts->attr->type;
					om->numAtts++;
					break;
				}
				i++;
				atts = atts->next;
			}
			sortAtt = sortAtt->next;
		}
		info->myOrder = om;
		info->runLength = RUNLEN;
		db->Create(dbpath, sorted, (void*) info);
	} else
		db->Create(dbpath, heap, NULL);
	db->Close();
	return 1;
}

int QueryPlan::ExecuteInsertFile(InsertFile *insertFile) {
	DBFile dbfile;
	char dbpath[100];
	sprintf(dbpath, "%s%s.bin", dbfile_dir, insertFile->tableName);
	dbfile.Open(dbpath);
	char fpath[100];
	sprintf(fpath, "%s%s", tpch_dir, insertFile->fileName);
	cout << "loading " << fpath << endl;
	Schema schema((char*) catalog_path, insertFile->tableName);
	dbfile.Load(schema, fpath);
	dbfile.Close();
	return 1;
}

int QueryPlan::ExecuteDropTable(char *dropTable) {
	char dbpath[100];
	sprintf(dbpath, "%s%s.bin", dbfile_dir, dropTable);
	remove(dbpath);
	sprintf(dbpath, "%s.header", dbpath);
	remove(dbpath);
}

//----------------------------------------------------------Execute------------------------------
int QueryPlan::ExecuteQueryPlan() {

	if (strcmp(this->outputType, "NONE") == 0) { //just print out the query plan
		this->root->PrintPostOrder();
	} else {
		this->root->PrintPostOrder();
		WriteOutQPNode *writeOut = new WriteOutQPNode(this->root->outPipeId,
				this->outputType, this->root->outputSchema);
		writeOut->left = this->root;
		writeOut->ExecutePostOrder();
		cout << "Starting Wait" << endl;
		int ctr = 0;
		for (vector<RelationalOp*>::iterator it =
				QueryPlanNode::relOpList.begin();
				it != QueryPlanNode::relOpList.end(); it++) {
			cout << ctr++ << endl;
			(*it)->WaitUntilDone();
		}
	}
	return 1;
}
