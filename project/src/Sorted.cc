#include "Sorted.h"

#include <cstdio>
#include <iostream>

#include "ComparisonEngine.h"
#include "File.h"
#include "Record.h"

Sorted::Sorted() {
	info = new SortInfo();
}

Sorted::~Sorted() {
	delete info;
}

int Sorted::Create(char *f_path, void *startup) {
	try {
		info = (SortInfo *) startup;
		myFile.Open(0, f_path);
		myFile.Close();
		return 1;
	} catch (int e) {
		cout << "An exception occurred. " << e << '\n';
		return 0;
	}
}

void Sorted::Load(Schema &f_schema, char *loadpath) {
	// now open up the text file and start procesing it
	FILE *tableFile = fopen(loadpath, "r");
	Record temp;
	Page p;
	// read in all of the records from the text file.
	int counter = 0, i = 0;
	while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
		counter++;
		if (!p.Append(&temp)) {
			myFile.AddPage(&p, i++);
			p.EmptyItOut();
			p.Append(&temp);
		}
	}
	myFile.AddPage(&p, i++);
	p.EmptyItOut();
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
	currentPage = -1;
	return 1;
}

void Sorted::MoveFirst() {
	currentPage = 0;
	myFile.GetPage(&readPageBuf, currentPage);
}

int Sorted::Close() {
	char *metaFile = new char[100];
	sprintf(metaFile, "%s.meta", fileName);
	FILE *meta = fopen(metaFile, "w");
	if (!meta) {
		cerr << "Open meta file error!" << endl;
		return 0;
	}
	fprintf(meta, "%d\n", 1);
	fprintf(meta, "%d\n", info->runLength);
	fprintf(meta, "%s", info->myOrder->ToString().c_str());
	fclose(meta);
	delete[] metaFile;
	if (myFile.Close()) {
		return 1;
	}
	return 0;
}

void Sorted::Add(Record &rec) {
	Record temp;
	temp.Consume(&rec);
	if (myFile.GetLength() == 0) {
		// if file is empty, add record to the new page and add page to File.
		writePageBuf.Append(&temp);
		myFile.AddPage(&writePageBuf, 0);
	} else {
		// if File is not empty, get the last page in the File.
		myFile.GetPage(&writePageBuf, myFile.GetLength() - 2);
		if (!writePageBuf.Append(&temp)) {
			// if Record cannot be added in the last page, Add the page back in its position.
			myFile.AddPage(&writePageBuf, myFile.GetLength() - 2);
			writePageBuf.EmptyItOut();
			// Add the record to a new page and append.
			writePageBuf.Append(&temp);
			myFile.AddPage(&writePageBuf, myFile.GetLength() - 1);
		} else {
			// If record can be added to the last page, add the page back to the File.
			myFile.AddPage(&writePageBuf, myFile.GetLength() - 2);
		}
		writePageBuf.EmptyItOut();
	}
}

int Sorted::GetNext(Record &fetchme) {
	// read next record from page buffer.
	if (!readPageBuf.GetFirst(&fetchme)) {
		// if read page buffer is empty.
		++currentPage;
		if (currentPage + 1 < myFile.GetLength()) {
			// get next page if available and read the record from there.
			myFile.GetPage(&readPageBuf, currentPage);
			readPageBuf.GetFirst(&fetchme);
		} else {
			// if reached the end of file. Do we need to check in the write buffer page also?
			return 0;
		}
	}
	return 1;
}

int Sorted::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
	// loop until we break; we break when a record is matched.
	while (1) {
		// read next record from page buffer.
		if (!readPageBuf.GetFirst(&fetchme)) {
			// if read page buffer is empty.
			++currentPage;
			if (currentPage + 1 < myFile.GetLength()) {
				// get next page if available and read the record from there.
				myFile.GetPage(&readPageBuf, currentPage);
				readPageBuf.GetFirst(&fetchme);
			} else {
				// if reached the end of file. Do we need to check in the write buffer page also?
				return 0;
			}
		}
		// check if record matches predicate;
		if (comp.Compare(&fetchme, &literal, &cnf)) {
			break;
		}
	}
	return 1;
}
