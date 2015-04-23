#include "QueryPlan.h"

using namespace std;
extern char *catalog_path;
extern char *dbfile_dir;
extern char *tpch_dir;

map<int, Pipe*> QueryPlanNode::pipesList;

void QueryPlanNode::ExecutePostOrder() {
	if (this->left)
		this->left->ExecutePostOrder();
	if (this->right)
		this->right->ExecutePostOrder();
	cout << "Pipe id" << this->outPipeId << endl;
	this->ExecuteNode();
}

void QueryPlanNode::CreatePipe() {
	pipesList[outPipeId] = new Pipe(QUERY_PIPE_SIZE);
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
	pipesList[outPipeId] = new Pipe(QUERY_PIPE_SIZE);
}

SelectPipeQPNode::~SelectPipeQPNode() {
	cout << "Decon SP" << endl;
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
	if (this->left != NULL)
		this->left->PrintNode();

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

	if (this->right != NULL)
		this->right->PrintNode();
}

void SelectPipeQPNode::ExecuteNode() {
	SelectPipe selPipe;
	selPipe.Use_n_Pages(QUERY_USE_PAGES);
	if (cnf != NULL && literal != NULL) {
		selPipe.Run(*(pipesList[leftInPipeId]), *(pipesList[outPipeId]), *cnf,
				*literal);
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
	pipesList[outPipeId] = new Pipe(QUERY_PIPE_SIZE);
}

SelectFileQPNode::~SelectFileQPNode() {
	cout << "Decon SF" << endl;
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
	if (this->left != NULL)
		this->left->PrintNode();

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
	if (this->right != NULL)
		this->right->PrintNode();
}

void SelectFileQPNode::ExecuteNode() {
	//create a DBFile from input file path provided
	DBFile * pFile = new DBFile;
	pFile->Open(const_cast<char*>(sFileName.c_str()));

	SelectFile * pSF = new SelectFile;
	pSF->Use_n_Pages(QUERY_USE_PAGES);
	if (cnf != NULL && literal != NULL) {
		cout << "************" << endl;
		cnf->Print();
		pSF->Run(*pFile, *(pipesList[outPipeId]), *cnf, *literal);
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
	pipesList[outPipeId] = new Pipe(QUERY_PIPE_SIZE);
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
	if (this->left != NULL)
		this->left->PrintNode();

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
	if (this->right != NULL)
		this->right->PrintNode();
}

void ProjectQPNode::ExecuteNode() {
	Project P;
	P.Use_n_Pages(QUERY_USE_PAGES);
	if (attributeList != NULL) {
		P.Run(*(pipesList[leftInPipeId]), *(pipesList[outPipeId]),
				attributeList, iTotalAttributes, iAtttributesToKeep);
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
	pipesList[outPipeId] = new Pipe(QUERY_PIPE_SIZE);
}

JoinQPNode::~JoinQPNode() {
}

void JoinQPNode::PrintNode() {
	if (this->left != NULL)
		this->left->PrintNode();
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
	if (this->right != NULL)
		this->right->PrintNode();
}

void JoinQPNode::ExecuteNode() {
	Join J;
	J.Use_n_Pages(QUERY_USE_PAGES);
	if (cnf != NULL && literal != NULL) {
		J.Run(*(pipesList[leftInPipeId]), *(pipesList[rightInPipeId]),
				*(pipesList[outPipeId]), *cnf, *literal);
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
	pipesList[outPipeId] = new Pipe(QUERY_PIPE_SIZE);
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
	if (this->left != NULL)
		this->left->PrintNode();

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

	if (this->right != NULL)
		this->right->PrintNode();
}

void GroupByQPNode::ExecuteNode() {
	GroupBy G;
	G.Use_n_Pages(QUERY_USE_PAGES);
	if (func != NULL && orderMaker != NULL) {
		G.Run(*(pipesList[leftInPipeId]), *(pipesList[outPipeId]), *orderMaker,
				*func);
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
	pipesList[outPipeId] = new Pipe(QUERY_PIPE_SIZE);
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
	if (this->left != NULL)
		this->left->PrintNode();

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

	if (this->right != NULL)
		this->right->PrintNode();
}

void SumQPNode::ExecuteNode() {
	Sum S;
	S.Use_n_Pages(QUERY_USE_PAGES);
	if (func != NULL) {
		S.Run(*(pipesList[leftInPipeId]), *(pipesList[outPipeId]), *func);
	}
}

// -------------------------------------- Distinct ------------------
DistinctQPNode::DistinctQPNode() {
}

DistinctQPNode::DistinctQPNode(int ip, int op, Schema * pSch) {
	leftInPipeId = ip;
	outPipeId = op;
	outputSchema = pSch;
	pipesList[outPipeId] = new Pipe(QUERY_PIPE_SIZE);
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
	if (this->left != NULL)
		this->left->PrintNode();

	cout << "\n*** Distinct Operation ***";
	cout << "\nInput pipe ID: " << leftInPipeId;
	cout << "\nOutput pipe ID: " << outPipeId;
	cout << "\nOutput Schema: ";
	if (outputSchema != NULL)
		outputSchema->Print();
	else
		cout << "NULL";

	cout << endl << endl;

	if (this->right != NULL)
		this->right->PrintNode();
}

void DistinctQPNode::ExecuteNode() {
	DuplicateRemoval DR;
	DR.Use_n_Pages(QUERY_USE_PAGES);
	if (outputSchema != NULL) {
		DR.Run(*(pipesList[leftInPipeId]), *(pipesList[outPipeId]),
				*outputSchema);
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
	if (this->left != NULL)
		this->left->PrintNode();

	cout << "\n*** WriteOut Operation ***";
	cout << "\nInput pipe ID: " << leftInPipeId;
	cout << "\nOutput file: " << outFileName;
	cout << "\nOutput Schema: ";
	if (outputSchema != NULL)
		outputSchema->Print();
	else
		cout << "NULL";

	cout << endl << endl;

	if (this->right != NULL)
		this->right->PrintNode();
}

void WriteOutQPNode::ExecuteNode() {
	WriteOut W;
	if (outputSchema != NULL && !outFileName.empty()) {
		FILE * pFILE;
		if (strcmp(outFileName.c_str(), "STDOUT") == 0) {
			pFILE = stdout;
		} else {
			pFILE = fopen((char*) outFileName.c_str(), "w");
		}
		W.Run(*(pipesList[leftInPipeId]), pFILE, *outputSchema);
		W.WaitUntilDone();
		fclose(pFILE);
	}
}

QueryPlan::QueryPlan() {
	this->pipeNum = 0;
	this->outputType = "STDOUT";
}

QueryPlan::~QueryPlan() {
}

void QueryPlan::PrintInOrder() {
	root->PrintNode();
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
		this->PrintInOrder();
	} else {
		this->PrintInOrder();
		WriteOutQPNode *writeOut = new WriteOutQPNode(this->root->outPipeId,
				this->outputType, this->root->outputSchema);
		writeOut->left = this->root;
		writeOut->ExecutePostOrder();
	}
	return 1;
}
