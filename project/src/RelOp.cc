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

void SelectFile::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int n) {
	this->nPages = n;
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
		delete tmpRecord;
		this->outPipe->ShutDown();
	}
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->cnf = &selOp;
	this->literal->Copy(&literal);
	pthread_create(&thread, NULL, Helper, this);
}

void SelectPipe::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void SelectPipe::Use_n_Pages(int n) {
	this->nPages = n;
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

void Project::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void Project::Use_n_Pages(int n) {
	this->nPages = n;
}

void *Join::Helper(void *arg) {
	Join *dm = (Join *) arg;
	dm->Apply();
	return NULL;
}

void Join::Apply() {
	Record *lRec = new Record();
	Record *rRec = new Record();
	ComparisonEngine comp;

	OrderMaker *leftOrder = new OrderMaker();
	OrderMaker *rightOrder = new OrderMaker();
	if (this->cnf->GetSortOrders(*leftOrder, *rightOrder)) {
		Pipe *leftSortPipe = new Pipe(PIPE_SIZE);
		BigQ *bigQL = new BigQ(*(this->inPipeL), *leftSortPipe, *leftOrder,
		RUNLEN);

		Pipe *rightSortPipe = new Pipe(PIPE_SIZE);
		BigQ *bigQR = new BigQ(*(this->inPipeR), *rightSortPipe, *rightOrder,
		RUNLEN);

		vector<Record *> vectorR;
		if (leftSortPipe->Remove(lRec) && rightSortPipe->Remove(rRec)) {
			// Get Number of Attributes from both relations.
			int leftAttr = lRec->GetNumAtts();
			int rightAttr = rRec->GetNumAtts();
			int attrToKeep[leftAttr + rightAttr];
			for (int i = 0; i < leftAttr; i++)
				attrToKeep[i] = i;
			for (int i = 0; i < rightAttr; i++)
				attrToKeep[i + leftAttr] = i;

			bool moreInLeft = true;
			// there are records in left relation.
			while (moreInLeft) {
				moreInLeft = false;
				// create a vector of records matching current record from right relation, only do this if records not already present.
				if (!vectorR.size()) {
					Record *rcd2 = new Record();
					rcd2->Consume(rRec);
					vectorR.push_back(rcd2);
					//get rcds from PipeR that equal to rRec
					while (rightSortPipe->Remove(rRec)) {
						if (0 == comp.Compare(rRec, rcd2, rightOrder)) { // equal
							Record *cRMe = new Record();
							cRMe->Consume(rRec);
							vectorR.push_back(cRMe);
						} else {
							// more in right = true;
							break;
						}
					}
				}

				int res = comp.Compare(lRec, leftOrder, vectorR.front(),
						rightOrder);
				if (res == 0) {
					Record *tmpRec = new Record, *jointRec = NULL;
					for (int i = 0; i < vectorR.size(); i++) {
						//join and push to output
						if (comp.Compare(lRec, vectorR.at(i), this->literal,
								this->cnf)) {
							tmpRec->Copy(vectorR.at(i));
							jointRec = new Record;
							jointRec->MergeRecords(lRec, tmpRec, leftAttr,
									rightAttr, attrToKeep, leftAttr + rightAttr,
									leftAttr);
							this->outPipe->Insert(jointRec);
						}
					}
					// get next record in left relation.
					if (leftSortPipe->Remove(lRec))
						moreInLeft = true;
				} else if (res > 0) {
					// L > R
					moreInLeft = true;
					vectorR.clear();
					if (!rightSortPipe->Remove(rRec))
						break;
				} else {
					// L < R
					if (leftSortPipe->Remove(lRec))
						moreInLeft = true;
				}
			}
		}
	} else { //----------Block Nested Loop Join-----------------
		// store part of left relation in a buffer to avoid bottleneck of the pipe. write the right relation to a DBFile.
		Page tmpPage;
		DBFile tmpDbFile;
		tmpDbFile.Create((char*) "tmpL", heap, NULL);
		tmpDbFile.Open("tmpL");

		if (this->inPipeL->Remove(lRec) && this->inPipeR->Remove(rRec)) {
			// Get Number of Attributes from both relations.
			int leftAttr = lRec->GetNumAtts();
			int rightAttr = rRec->GetNumAtts();
			int attrToKeep[leftAttr + rightAttr];
			for (int i = 0; i < leftAttr; i++)
				attrToKeep[i] = i;
			for (int i = 0; i < rightAttr; i++)
				attrToKeep[i + leftAttr] = i;

			// add all the recs to a DBFile
			do {
				tmpDbFile.Add(*rRec);
			} while (this->inPipeR->Remove(rRec));

			vector<Record *> buffer;

			bool moreInLeft = true;
			while (moreInLeft) {
				moreInLeft = false;
				Record *first = new Record();
				first->Copy(lRec);
				buffer.push_back(first);
				tmpPage.Append(lRec);
				int pgCnt = 0;

				while (this->inPipeL->Remove(lRec)) {
					//getting n-1 pages of records into vectorR
					Record *copyMe = new Record();
					copyMe->Copy(lRec);
					if (!tmpPage.Append(lRec)) {
						pgCnt += 1;
						if (pgCnt >= this->nPages - 1) {
							moreInLeft = true;
							break;
						} else {
							tmpPage.EmptyItOut();
							tmpPage.Append(lRec);
							buffer.push_back(copyMe);
						}
					} else {
						buffer.push_back(copyMe);
					}
				}

				tmpDbFile.MoveFirst(); //we should do this in each iteration, iterate all the tuples in Left
				while (tmpDbFile.GetNext(*rRec)) {
					for (int i = 0; i < buffer.size(); i++) {
						if (comp.Compare(buffer.at(i), rRec, this->literal,
								this->cnf)) {
							//applied to the CNF, then join
							Record *jointRec = new Record();
							Record *tempRec = new Record();
							tempRec->Copy(buffer.at(i));
							jointRec->MergeRecords(tempRec, rRec, leftAttr,
									rightAttr, attrToKeep, leftAttr + rightAttr,
									leftAttr);
							this->outPipe->Insert(jointRec);
						}
					}
				}
				//clean up the vectorR
				buffer.clear();
			}
			tmpDbFile.Close();
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

void Join::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void Join::Use_n_Pages(int n) {
	this->nPages = n;
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

void Sum::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void Sum::Use_n_Pages(int n) {
	this->nPages = n;
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
void DuplicateRemoval::WaitUntilDone() {
	pthread_join(thread, NULL);
}
void DuplicateRemoval::Use_n_Pages(int n) {
	this->nPages = n;
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
			this->nPages);
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
void GroupBy::WaitUntilDone() {
	pthread_join(thread, NULL);
}
void GroupBy::Use_n_Pages(int n) {
	this->nPages = n;
}

void *WriteOut::Helper(void *arg) {
	WriteOut *wo = (WriteOut *) arg;
	wo->Apply();
	return NULL;
}

void WriteOut::Apply() {
	Record *tmpRecord = new Record;
	ComparisonEngine comp;
	while (this->inPipe->Remove(tmpRecord)) {
		tmpRecord->Print(this->mySchema, (this->outFile));
	}
	delete tmpRecord;
	fclose(this->outFile);
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;
	pthread_create(&thread, NULL, Helper, this);
}

void WriteOut::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages(int n) {
	this->nPages = n;
}
