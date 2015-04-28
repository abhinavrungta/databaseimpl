#ifndef REL_OP_H
#define REL_OP_H

#include <pthread.h>
#include <cstdio>
#include <cstdlib>

#include "Comparison.h"
#include "Pipe.h"
#include "Function.h"

class DBFile;

class RelationalOp {
public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone();

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages(int n);

	virtual void Apply() = 0;

protected:
	pthread_t thread;  // the thread the operator will use.
	int nPages;
};

class SelectFile: public RelationalOp {
	static void* Helper(void*);
private:
	DBFile *inFile;
	Pipe *outPipe;
	CNF *cnf;
	Record *literal;
public:
	void Run(DBFile &inFile, Pipe &outPipe, CNF &cnf, Record &literal);
	void Apply();
};

class SelectPipe: public RelationalOp {
	static void* Helper(void*);
private:
	Pipe *inPipe, *outPipe;
	CNF *cnf;
	Record *literal;
public:
	void Run(Pipe &inPipe, Pipe &outPipe, CNF &cnf, Record &literal);
	void Apply();
};

class Project: public RelationalOp {
	static void* Helper(void*);
private:
	Pipe *inPipe, *outPipe;
	int *keepMe;
	int numAttsInput, numAttsOutput;
public:
	void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput,
			int numAttsOutput);
	void Apply();
};

class Join: public RelationalOp {
	static void* Helper(void*);
private:
	Pipe *inPipeL, *inPipeR, *outPipe;
	CNF *cnf;
	Record *literal;

public:
	void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &cnf,
			Record &literal);
	void Apply();
};
class DuplicateRemoval: public RelationalOp {
	static void* Helper(void*);
private:
	Pipe *inPipe, *outPipe;
	Schema *mySchema;
public:
	void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void Apply();
};
class Sum: public RelationalOp {
	static void* Helper(void*);
private:
	Pipe *inPipe, *outPipe;
	Function *computeMe;
public:
	void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void Apply();
};
class GroupBy: public RelationalOp {
	static void* Helper(void*);
private:
	Pipe *inPipe, *outPipe;
	OrderMaker *groupAtts;
	Function *computeMe;
public:
	void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
			Function &computeMe);
	void Apply();
};
class WriteOut: public RelationalOp {
	static void* Helper(void*);
private:
	Pipe *inPipe;
	FILE *outFile;
	Schema *mySchema;

public:
	void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void Apply();
};
#endif
