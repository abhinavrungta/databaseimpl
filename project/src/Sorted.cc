#include "Sorted.h"

#include <cstdio>
#include <iostream>
#include <string>

#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "File.h"
#include "Pipe.h"
#include "Record.h"

Sorted::Sorted() {
	info = new SortInfo();
	input = NULL;
	output = NULL;
	bigQ = NULL;
	mergePageCtr = 0;
	queryChanged = true;
	queryOrder = NULL;
	literalOrder = NULL;
}

Sorted::~Sorted() {
	delete info;
}

int Sorted::Create(char *f_path, void *startup) {
	try {
		info = (SortInfo *) startup;
		myFile.Open(0, f_path);
		return 1;
	} catch (int e) {
		cout << "An exception occurred. " << e << '\n';
		return 0;
	}
}

void Sorted::Load(Schema &f_schema, char *loadpath) {
	// now open up the text file and start processing it
	FILE *tableFile = fopen(loadpath, "r");
	Record temp;
	// read in all of the records from the text file.
	int counter = 0, i = 0;
	while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
		counter++;
		Add(temp);
	}
}

int Sorted::Open(char *f_path) {
	//retrieve sort info from file header
	char *metaFile = new char[100];
	int runlen, attNum;
	sprintf(metaFile, "%s.meta", fileName);
	FILE *meta = fopen(metaFile, "r");
	OrderMaker *tmp = new OrderMaker;
	// runlen is in second line, so read twice.
	fscanf(meta, "%d", &runlen);
	fscanf(meta, "%d", &runlen);
	info->runLength = runlen;
	fscanf(meta, "%d", &attNum);
	tmp->numAtts = attNum;
	for (int i = 0; i < attNum; i++) {
		int att;
		int type;
		if (feof(meta)) {
			cerr << "Retrieve OrderMaker error" << endl;
			return 0;
		}
		fscanf(meta, "%d %d", &att, &type);
		tmp->whichAtts[i] = att;
		if (0 == type) {
			tmp->whichTypes[i] = Int;
		} else if (1 == type) {
			tmp->whichTypes[i] = Double;
		} else
			tmp->whichTypes[i] = String;
	}
	info->myOrder = tmp;
	fclose(meta);
	delete[] metaFile;

	// open file.
	myFile.Open(1, f_path);
	readPageCtr = -1;
	readPageBuf.EmptyItOut();
	writePageCtr = myFile.GetLength() == 0 ? 0 : myFile.GetLength() - 2;
	return 1;
}

void Sorted::MoveFirst() {
	readPageCtr = -1;
	readPageBuf.EmptyItOut();
	queryChanged = true;
}

int Sorted::Close() {
	if (mode) {
		mode = 0;
		// if switching from write mode, close input pipe and merge bigQ with already sorted file.
		MergeBigQ();
	}
	// write meta info to the file.
	char *metaFile = new char[100];
	sprintf(metaFile, "%s.meta", fileName);
	FILE *meta = fopen(metaFile, "w");
	if (!meta) {
		cerr << "Open meta file error!" << endl;
		return 0;
	}
	fprintf(meta, "%d\n", 0);
	fprintf(meta, "%d\n", info->runLength);
	fprintf(meta, "%s", info->myOrder->ToString().c_str());
	fclose(meta);
	delete[] metaFile;
	// close file.
	myFile.Close();
	return 1;
}

void Sorted::Add(Record &rec) {
	queryChanged = true;
	Record temp;
	temp.Consume(&rec);
	if (!mode) {
		// if switching from read mode, Create instance of BigQ.
		mode = 1;
		if (input == NULL)
			input = new Pipe(PIPE_SIZE);
		if (output == NULL)
			output = new Pipe(PIPE_SIZE);
		if (bigQ == NULL)
			bigQ = new BigQ(*input, *output, *(info->myOrder), info->runLength);
	}
	input->Insert(&temp);
}

int Sorted::GetNext(Record &fetchme) {
	if (mode) {
		mode = 0;
		// if switching from write mode, close input pipe and merge bigQ with already sorted file.
		MergeBigQ();
		MoveFirst();
	}
	// read next record from page buffer.
	if (!readPageBuf.GetFirst(&fetchme)) {
		// check if current page is not last page, fetch next page.
		if (readPageCtr < myFile.GetLength() - 2) {
			++readPageCtr;
			myFile.GetPage(&readPageBuf, readPageCtr);
			readPageBuf.GetFirst(&fetchme);
		} else {
			return 0;
		}
	}
	return 1;
}

int Sorted::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	if (mode) {
		// if switching from write mode, close input pipe and merge bigQ with already sorted file.
		mode = 0;
		MergeBigQ();
		MoveFirst();
	}
	ComparisonEngine comp;
	// construct new QueryOrder if queryChanged.
	if (queryChanged) {
		cout << "Query Changed" << endl;
		queryChanged = false;
		queryOrder = cnf.getQueryOrder(*(info->myOrder), &literalOrder);
		// if queryOrder created, do a binary search.
		if (queryOrder != NULL) {
			cout << "Query Order Created. Doing Binary Search" << endl;
			int low = readPageCtr == -1 ? 0 : readPageCtr;
			int high = myFile.GetLength() - 2;
			int pageNo = BinarySearch(low, high, queryOrder, literalOrder,
					literal); // returns page no with matching queryorder.
			// if no possible match for given queryOrder.
			if (pageNo == -1) {
				cout << "Page Not found";
				return 0;
			}
			// load the searched page if not equal to page in buffer.
			if (pageNo != readPageCtr) {
				readPageBuf.EmptyItOut();
				myFile.GetPage(&readPageBuf, pageNo);
				readPageCtr = pageNo;
			}
			// get the first probable record which matches query order in current page or next one.
			while (GetNext(fetchme) && readPageCtr <= pageNo + 1) {
				if (comp.Compare(&fetchme, queryOrder, &literal, literalOrder)
						== 0)
					break;
			}
			//if the first record matches the CNF also, return 1.
			if (comp.Compare(&fetchme, &literal, &cnf)) {
				cout << "First Match Found." << endl;
				return 1;
			}
		}
	}
	// if query order is not null, and the first match found above did not satisfy the CNF or we are here because query was not changed, do a linear scan.
	if (queryOrder != NULL) {
		while (GetNext(fetchme)) {
			// at this point if queryOrder is not matching, return 0;
			if (comp.Compare(&fetchme, queryOrder, &literal, literalOrder)
					!= 0) {
				return 0;
			} else {
				if (comp.Compare(&fetchme, &literal, &cnf))
					return 1; //found the right record, return 1.
			}
		}
	} else {
		cout << "No Query Order constructed" << endl;
		while (GetNext(fetchme)) {
			if (comp.Compare(&fetchme, &literal, &cnf))
				return 1;
		}
	}
	return 0;
}

OrderMaker* Sorted::GetSortOrder() {
	return info->myOrder;
}

void Sorted::addToFile(Record &temp) {
	if (!mergePageBuf.Append(&temp)) {
		tmpFile.AddPage(&mergePageBuf, mergePageCtr);
		mergePageBuf.EmptyItOut();
		mergePageBuf.Append(&temp);
		mergePageCtr++;	// increase the counter whenever record is appended to new page.
	}
}

void Sorted::MergeBigQ() {
	input->ShutDown();
	cout << "Merging BigQ" << endl;
	Record *rec1 = new Record;
	Record *rec2 = new Record;

	char *tmpName = new char[100];
	sprintf(tmpName, "%s.tmp", this->fileName);
	tmpFile.Open(0, tmpName);
	tmpFile.Close();
	tmpFile.Open(1, tmpName);
	mergePageBuf.EmptyItOut();
	mergePageCtr = 0;
	bool flagFile = false;
	bool flagBigQ = false;

	ComparisonEngine cmp;
	this->MoveFirst();

	if (output->Remove(rec1)) {
		flagBigQ = true;
	}
	if (this->GetNext(*rec2)) {
		flagFile = true;
	}
	while (flagBigQ && flagFile) {
		if (cmp.Compare(rec1, rec2, info->myOrder) <= 0) {
			addToFile(*rec1);
			if (!output->Remove(rec1)) {
				flagBigQ = false;
			}
		} else {
			addToFile(*rec2);
			if (!this->GetNext(*rec2)) {
				flagFile = false;
			}
		}
	}

	if (flagBigQ) {
		do {
			addToFile(*rec1);
		} while (output->Remove(rec1));
	}

	if (flagFile) {
		do {
			addToFile(*rec2);
		} while (this->GetNext(*rec2));
	}

	// add the buffer to file if needed
	if (mergePageBuf.GetLength() > 0) {
		tmpFile.AddPage(&mergePageBuf, mergePageCtr);
		mergePageBuf.EmptyItOut();
	}

	// clean up
	delete rec1;
	delete rec2;
	delete input;
	delete output;

	myFile.Close();
	tmpFile.Close();
	if (remove(fileName)) {
		return;
	}
	if (rename(tmpName, fileName)) {
		return;
	}
	myFile.Open(1, fileName);
}

int Sorted::BinarySearch(int low, int high, OrderMaker *order,
		OrderMaker *lorder, Record &literal) {
	ComparisonEngine comp;
	if (high < low)
		return -1;
	if (high == low)
		return low;

	Page *tmpPage = new Page;
	Record *tmpRecord = new Record;
	int mid = (int) (high + low) / 2;
	myFile.GetPage(tmpPage, mid);
	tmpPage->GetFirst(tmpRecord);
	int res = comp.Compare(tmpRecord, order, &literal, lorder);
	delete tmpPage;
	delete tmpRecord;

	if (res < 0) {
		if (low == mid)
			// to avoid recursion.
			return mid;
		else
			return BinarySearch(mid, high, order, lorder, literal);
	} else if (res == 0) {
		if (low == mid)
			return low;	// to avoid return of -1 in successive call;
		return BinarySearch(low, mid - 1, order, lorder, literal);// go here to find the first page with equal record.
	} else
		return BinarySearch(low, mid - 1, order, lorder, literal);
}
