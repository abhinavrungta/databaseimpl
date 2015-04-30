#include "RelOp.h"

#include <pthread.h>
#include <iostream>
#include <string>
#include <sstream>

#include "BigQ.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Record.h"
#include "Schema.h"

#define CLEANUPVECTOR(v) \
	({ for(vector<Record *>::iterator it = v.begin(); it!=v.end(); it++) { \
		if(!*it) { delete *it; } }\
		v.clear();\
})

void RelationalOp::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void RelationalOp::Use_n_Pages(int n) {
	this->nPages = n;
}

void *SelectFile::Helper(void *arg) {
	SelectFile *sf = (SelectFile *) arg;
	sf->Apply();
	return NULL;
}

void SelectFile::Apply() {
	Record *tmpRecord = new Record;
	// get Record from input DBFile, compare with given CNF and push to output pipe.
	while (this->inFile->GetNext(*tmpRecord, *(this->cnf), *(this->literal))) {
		this->outPipe->Insert(tmpRecord);
	}
	delete tmpRecord;
	this->inFile->Close();
	this->outPipe->ShutDown();
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp,
		Record &literal) {
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->cnf = &selOp;
	this->literal = &literal;
	pthread_create(&thread, NULL, Helper, this);
}

void *SelectPipe::Helper(void *arg) {
	SelectPipe *sf = (SelectPipe *) arg;
	sf->Apply();
	return NULL;
}

void SelectPipe::Apply() {
	Record *tmpRecord = new Record;
	ComparisonEngine comp;
	// get Record from input pipe, compare with given CNF and push to output pipe.
	while (this->inPipe->Remove(tmpRecord)) {
		if (comp.Compare(tmpRecord, this->literal, this->cnf)) {
			this->outPipe->Insert(tmpRecord);
		}
	}
	delete tmpRecord;
	this->outPipe->ShutDown();
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->cnf = &selOp;
	this->literal = new Record();
	this->literal->Copy(&literal);
	pthread_create(&thread, NULL, Helper, this);
}

void *Project::Helper(void *arg) {
	Project *pr = (Project *) arg;
	pr->Apply();
	return NULL;
}

void Project::Apply() {
	Record *tmpRecord = new Record;
	ComparisonEngine comp;
	// get record from input pipe, project the given attributes and add to output pipe.
	while (this->inPipe->Remove(tmpRecord)) {
		tmpRecord->Project(keepMe, numAttsOutput, numAttsInput);
		this->outPipe->Insert(tmpRecord);
	}
	delete tmpRecord;
	this->outPipe->ShutDown();
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput,
		int numAttsOutput) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	pthread_create(&thread, NULL, Helper, this);
}

void *Join::Helper(void *arg) {
	Join *dm = (Join *) arg;
	dm->Apply();
	return NULL;
}

void Join::Apply() {
	OrderMaker orderL;
	OrderMaker orderR;
	this->cnf->GetSortOrders(orderL, orderR);

	//----------sort-merge Join-----------
	if (orderL.numAtts && orderR.numAtts && orderL.numAtts == orderR.numAtts) {
		//means we can do a sort-merge join
		Pipe pipeL(PIPE_SIZE), pipeR(PIPE_SIZE);
		BigQ *bigQL = new BigQ(*(this->inPipeL), pipeL, orderL, RUNLEN);
		BigQ *bigQR = new BigQ(*(this->inPipeR), pipeR, orderR, RUNLEN);

		//next get tuples out in order from pipeL and pipeR, and put the equal tuples into two vectors
		//then cross-multiply them
		vector<Record *> vectorL;
		vector<Record *> vectorR;
		Record *rcdL = new Record();
		Record *rcdR = new Record();
		ComparisonEngine cmp;

		if (pipeL.Remove(rcdL) && pipeR.Remove(rcdR)) {
			int leftAttr = ((int *) rcdL->bits)[1] / sizeof(int) - 1;
			int rightAttr = ((int *) rcdR->bits)[1] / sizeof(int) - 1;
			int totalAttr = leftAttr + rightAttr;
			int attrToKeep[totalAttr];
			for (int i = 0; i < leftAttr; i++)
				attrToKeep[i] = i;
			for (int i = 0; i < rightAttr; i++)
				attrToKeep[i + leftAttr] = i;
			int joinNum;

			bool leftOK = true, rightOK = true; //means that rcdL and rcdR are both ok
			int num = 0;
			while (leftOK && rightOK) {
				leftOK = false;
				rightOK = false;
				int cmpRst = cmp.Compare(rcdL, &orderL, rcdR, &orderR);
				switch (cmpRst) {
				case 0: // L == R
				{
					num++;
					Record *rcd1 = new Record();
					rcd1->Consume(rcdL);
					Record *rcd2 = new Record();
					rcd2->Consume(rcdR);
					vectorL.push_back(rcd1);
					vectorR.push_back(rcd2);

					//get rcds from pipeL that equal to rcdL
					while (pipeL.Remove(rcdL)) {
						if (0 == cmp.Compare(rcdL, rcd1, &orderL)) { // equal
							Record *cLMe = new Record();
							cLMe->Consume(rcdL);
							vectorL.push_back(cLMe);
						} else {
							leftOK = true;
							break;
						}
					}
					//get rcds from PipeR that equal to rcdR
					while (pipeR.Remove(rcdR)) {
						if (0 == cmp.Compare(rcdR, rcd2, &orderR)) { // equal
							Record *cRMe = new Record();
							cRMe->Consume(rcdR);
							vectorR.push_back(cRMe);
						} else {
							rightOK = true;
							break;
						}
					}
					//now we have the two vectors that can do cross product
					Record *lr = new Record, *rr = new Record, *jr = new Record;
					for (vector<Record *>::iterator itL = vectorL.begin();
							itL != vectorL.end(); itL++) {
						lr->Consume(*itL);
						for (vector<Record *>::iterator itR = vectorR.begin();
								itR != vectorR.end(); itR++) {
							//join and output
							if (1
									== cmp.Compare(lr, *itR, this->literal,
											this->cnf)) {
								joinNum++;
								rr->Copy(*itR);
								jr->MergeRecords(lr, rr, leftAttr, rightAttr,
										attrToKeep, leftAttr + rightAttr,
										leftAttr);
								this->outPipe->Insert(jr);
							}
						}
					}
					CLEANUPVECTOR(vectorL);
					CLEANUPVECTOR(vectorR);

					break;
				}
				case 1: // L > R
					leftOK = true;
					if (pipeR.Remove(rcdR))
						rightOK = true;
					break;
				case -1: // L < R
					rightOK = true;
					if (pipeL.Remove(rcdL))
						leftOK = true;
					break;
				}
			}
		}
	} else { //----------Block Nested Loop Join-----------------
		//assume the size of left relation is less than right relation
		int n_pages = 10;
		// take n_pages-1 pages from right, and 1 page from left
		Record *rcdL = new Record;
		Record *rcdR = new Record;
		Page pageR;
		DBFile dbFileL;
		fType ft = heap;
		dbFileL.Create((char*) "tmpL", ft, NULL);
		dbFileL.MoveFirst();

		int leftAttr, rightAttr, totalAttr, *attrToKeep;

		if (this->inPipeL->Remove(rcdL) && this->inPipeR->Remove(rcdR)) {
			//figure out the attributes of LHS record and RHS record
			leftAttr = ((int *) rcdL->bits)[1] / sizeof(int) - 1;
			rightAttr = ((int *) rcdR->bits)[1] / sizeof(int) - 1;
			totalAttr = leftAttr + rightAttr;
			attrToKeep = new int[totalAttr];
			for (int i = 0; i < leftAttr; i++)
				attrToKeep[i] = i;
			for (int i = 0; i < rightAttr; i++)
				attrToKeep[i + leftAttr] = i;
			do {
				dbFileL.Add(*rcdL);
			} while (this->inPipeL->Remove(rcdL));

			vector<Record *> vectorR;
			ComparisonEngine cmp;

			bool rMore = true;
			int joinNum = 0;
			while (rMore) {
				Record *first = new Record();
				first->Copy(rcdR);
				pageR.Append(rcdR);
				vectorR.push_back(first);
				int rPages = 0;

				rMore = false;
				while (this->inPipeR->Remove(rcdR)) {
					Record *copyMe = new Record();
					copyMe->Copy(rcdR);
					if (!pageR.Append(rcdR)) {
						rPages += 1;
						if (rPages >= n_pages - 1) {
							rMore = true;
							break;
						} else {
							pageR.EmptyItOut();
							pageR.Append(rcdR);
							vectorR.push_back(copyMe);
						}
					} else {
						vectorR.push_back(copyMe);
					}
				}
				dbFileL.MoveFirst(); //we should do this in each iteration
				//iterate all the tuples in Left
				int fileRN = 0;
				while (dbFileL.GetNext(*rcdL)) {
					for (vector<Record*>::iterator it = vectorR.begin();
							it != vectorR.end(); it++) {
						if (1
								== cmp.Compare(rcdL, *it, this->literal,
										this->cnf)) {
							//applied to the CNF, then join
							joinNum++;
							Record *jr = new Record();
							Record *rr = new Record();
							rr->Copy(*it);
							jr->MergeRecords(rcdL, rr, leftAttr, rightAttr,
									attrToKeep, leftAttr + rightAttr, leftAttr);
							this->outPipe->Insert(jr);
						}
					}
				}
				//clean up the vectorR
				CLEANUPVECTOR(vectorR);
			}
			dbFileL.Close();
		}
	}
	this->outPipe->ShutDown();
}
void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp,
		Record &literal) {
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->cnf = &selOp;
	this->literal = &literal;
	pthread_create(&thread, NULL, Helper, this);
}

void *Sum::Helper(void *arg) {
	Sum *sum = (Sum *) arg;
	sum->Apply();
	return NULL;
}

void Sum::Apply() {
	Record *tmpRecord = new Record;
	ostringstream ss;
// Construct Attribute Class for the new tuple to later generate a schema.
	Attribute *attr = new Attribute;
	attr->name = (char *) "sum";

// Get Result Type.
	Type type = this->computeMe->resultType();
// Based on the type, compute the result, assign attribute type.
	if (type == Int) {
		int result = 0;
		while (this->inPipe->Remove(tmpRecord)) {
			result += this->computeMe->Apply<int>(*tmpRecord);
		}
		attr->myType = Int;
		ss << result;
		ss << "|";
	} else if (type == Double) {
		double result = 0.0;
		while (this->inPipe->Remove(tmpRecord)) {
			result += this->computeMe->Apply<double>(*tmpRecord);
		}
		attr->myType = Double;
		ss << result;
		ss << "|";
	}

// create schema from attribute data.
	Schema *schema = new Schema((char *) "dummy", 1, attr);

// create a tempFile with the computed result, to read into a record at a later point.
	FILE * meta = fopen("tmpRelFile", "w");
	fputs(ss.str().c_str(), meta);
	fclose(meta);
	FILE * tmp = fopen("tmpRelFile", "r");
	tmpRecord->SuckNextRecord(schema, tmp);

// insert record and shutdown pipe.
	this->outPipe->Insert(tmpRecord);
	this->outPipe->ShutDown();
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe;
	pthread_create(&thread, NULL, Helper, this);
}

void *DuplicateRemoval::Helper(void *arg) {
	DuplicateRemoval *dm = (DuplicateRemoval *) arg;
	dm->Apply();
	return NULL;
}

void DuplicateRemoval::Apply() {
// get OrderMaker for all Attributes in the schema.
	OrderMaker *order = new OrderMaker(this->mySchema);
	if (!order)
		exit(0);
// create a BigQ instance to Sort the input.
	Pipe *sortPipe = new Pipe(PIPE_SIZE);
	BigQ *bigQ = new BigQ(*(this->inPipe), *sortPipe, *order, RUNLEN);

	Record *curr = new Record;
	Record *prev = new Record;
	ComparisonEngine comp;
// get output from BigQ.
	while (sortPipe->Remove(curr)) {
		if (prev->GetBits() != NULL && curr->GetBits() != NULL) {
			// if it is not equal to the previous record, then add to the output pipe, else do nothing.
			if (comp.Compare(prev, curr, order) != 0) {
				prev->Copy(curr);
				this->outPipe->Insert(curr);
			}
		} else if (prev->GetBits() == NULL) {
			prev->Copy(curr);
			this->outPipe->Insert(curr);
		}
	}
	delete curr;
	delete prev;
// shutdown pipe.
	this->outPipe->ShutDown();
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema;
	pthread_create(&thread, NULL, Helper, this);
}

void *GroupBy::Helper(void *arg) {
	GroupBy *gb = (GroupBy *) arg;
	gb->Apply();
	return NULL;
}

void GroupBy::Apply() {
	int numAttsToKeep = this->groupAtts->numAtts + 1;
	int *attsToKeep = new int[numAttsToKeep];
	attsToKeep[0] = 0;  //for sumRec
	for (int i = 1; i < numAttsToKeep; i++) {
		attsToKeep[i] = this->groupAtts->whichAtts[i - 1];
	}

	Pipe sortPipe(PIPE_SIZE);
	BigQ *bigQ = new BigQ(*(this->inPipe), sortPipe, *(this->groupAtts),
	RUNLEN);
	// Construct Attribute Class for the new tuple to later generate a schema.
	Attribute *attr = new Attribute;
	attr->name = (char *) "sum";
	// Get Result Type.
	Type type = this->computeMe->resultType();

	ComparisonEngine cmp;
	Record *tmpRcd = new Record();
	if (sortPipe.Remove(tmpRcd)) {
		bool more = true;
		while (more) {
			//new group
			more = false;
			Record *r = new Record();
			Record *lastRcd = new Record;
			ostringstream ss;
			if (type == Int) {
				int result = 0;
				result += this->computeMe->Apply<int>(*tmpRcd);
				lastRcd->Copy(tmpRcd);
				while (sortPipe.Remove(r)) {
					if (cmp.Compare(lastRcd, r, this->groupAtts) == 0) { //same group
						result = this->computeMe->Apply<int>(*r);
					} else {
						tmpRcd->Copy(r);
						more = true;
						break;
					}
				}
				attr->myType = Int;
				ss << result;
				ss << "|";
			} else if (type == Double) {
				double result = 0.0;
				result += this->computeMe->Apply<double>(*tmpRcd);
				lastRcd->Copy(tmpRcd);
				while (sortPipe.Remove(r)) {
					if (cmp.Compare(lastRcd, r, this->groupAtts) == 0) { //same group
						result = this->computeMe->Apply<double>(*r);
					} else {
						tmpRcd->Copy(r);
						more = true;
						break;
					}
				}
				attr->myType = Double;
				ss << result;
				ss << "|";
			}
			// create schema from attribute data.
			Schema *schema = new Schema((char *) "dummy", 1, attr);
			Record *sumRcd = new Record();
			// create a tempFile with the computed result, to read into a record at a later point.
			FILE * meta = fopen("tmpGrpFile", "w");
			fputs(ss.str().c_str(), meta);
			fclose(meta);
			FILE * tmp = fopen("tmpGrpFile", "r");
			sumRcd->SuckNextRecord(schema, tmp);
			fclose(tmp);

			Record *tuple = new Record;
			tuple->MergeRecords(sumRcd, lastRcd, 1, this->groupAtts->numAtts,
					attsToKeep, numAttsToKeep, 1);

			this->outPipe->Insert(tuple);
		}
	}
	this->outPipe->ShutDown();
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
		Function &computeMe) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;
	pthread_create(&thread, NULL, Helper, this);
}

void *WriteOut::Helper(void *arg) {
	WriteOut *wo = (WriteOut *) arg;
	wo->Apply();
	return NULL;
}

void WriteOut::Apply() {
	Record *tmpRecord = new Record;
	ComparisonEngine comp;
	int ctr = 0;
	while (this->inPipe->Remove(tmpRecord)) {
		tmpRecord->Print(this->mySchema, (this->outFile));
		ctr++;
	}
	cout << "# of Records " << ctr << endl;
	delete tmpRecord;
	if (this->outFile != stdout) {
		fclose(this->outFile);
	}

}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;
	pthread_create(&thread, NULL, Helper, this);
}
