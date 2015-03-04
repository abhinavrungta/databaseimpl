#include "Sorted.h"

#include <cstdio>
#include <iostream>

#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "File.h"
#include "Record.h"

Sorted::Sorted() {
	info = new SortInfo();
	input = NULL;
	output = NULL;
	bigQ = NULL;
	mergePageCtr = 0;
}

Sorted::~Sorted() {
	delete info;
	delete input;
	delete output;
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

	myFile.Open(1, f_path);
	readPageCtr = -1;
	readPageBuf.EmptyItOut();
	writePageCtr = myFile.GetLength() == 0 ? 0 : myFile.GetLength() - 2;
	return 1;
}

void Sorted::MoveFirst() {
	readPageCtr = -1;
	readPageBuf.EmptyItOut();
}

int Sorted::Close() {
	if (mode) {
		mode = 0;
		// if switching from write mode, close input pipe and merge bigQ with already sorted file.
		MergeBigQ();
	}
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
	myFile.Close();
	return 1;
}

void Sorted::Add(Record &rec) {
	Record temp;
	temp.Consume(&rec);
	if (!mode) {
		// if switching from read mode, get last page.
		if (input == NULL)
			input = new Pipe(100);
		if (output == NULL)
			output = new Pipe(100);
		if (bigQ == NULL)
			bigQ = new BigQ(*input, *output, *(info->myOrder), info->runLength);
		mode = 1;
	}
	input->Insert(&temp);
}

int Sorted::GetNext(Record &fetchme) {
	if (mode) {
		mode = 0;
		// if switching from write mode, close input pipe and merge bigQ with already sorted file.
		MergeBigQ();
	}
	// read next record from page buffer.
	if (!readPageBuf.GetFirst(&fetchme)) {
		// check if current page is not last page, fetch next page.
		if (readPageCtr < myFile.GetLength() - 2) {
			++readPageCtr;
			myFile.GetPage(&readPageBuf, readPageCtr);
			// if readBuf is out of sync, do not reset readCtr.
			if (readBufOutOfSync) {
				readBufOutOfSync = false;
			} else {
				readCtr = 0;
			}
			// if read buffer is out of sync, fetch records uptil last ctr.
			if (readCtr < readPageBuf.GetLength()) {
				int i = 0;
				while (i < readCtr) {
					readPageBuf.GetFirst(&fetchme);
					i++;
				}
			}
			readPageBuf.GetFirst(&fetchme);
		} else {
			return 0;
		}
	}
// increment record counter in current buffer.
	readCtr++;
	return 1;
}

int Sorted::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
// loop until we break; we break when a record is matched.
	while (1) {
		// read next record from page buffer.
		if (!GetNext(fetchme)) {
			return 0;
		}
		// check if record matches predicate;
		if (comp.Compare(&fetchme, &literal, &cnf)) {
			return 1;
		}
	}
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
	Record *rec1 = new Record;
	Record *rec2 = new Record;

	tmpFile.Open(0, "tmpFile");
	tmpFile.Close();
	tmpFile.Open(1, "tmpFile");
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

//add the buffer to file if needed
	if (mergePageBuf.GetLength() > 0) {
		tmpFile.AddPage(&mergePageBuf, mergePageCtr);
		mergePageBuf.EmptyItOut();
	}

//clean up
	delete rec1;
	delete rec2;

	myFile.Close();
	tmpFile.Close();
	if (remove(fileName)) {
		return;
	}
	if (rename("tmpFile", fileName)) {
		return;
	}
	myFile.Open(1, fileName);
}
