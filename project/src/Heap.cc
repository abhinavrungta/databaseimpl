#include "Heap.h"

#include <cstdio>
#include <iostream>

#include "ComparisonEngine.h"
#include "File.h"
#include "Record.h"

Heap::Heap() {
}

Heap::~Heap() {
}

int Heap::Create(char *f_path, void *startup) {
	try {
		myFile.Open(0, f_path);
		return 1;
	} catch (int e) {
		cout << "An exception occurred. Exception Nr. " << e << '\n';
		return 0;
	}
}

void Heap::Load(Schema &f_schema, char *loadpath) {
	// now open up the text file and start processing it
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

int Heap::Open(char *f_path) {
	myFile.Open(1, f_path);
	readPageCtr = -1;
	readPageBuf.EmptyItOut();
	writePageCtr = myFile.GetLength() == 0 ? 0 : myFile.GetLength() - 2;
	return 1;
}

void Heap::MoveFirst() {
	readPageCtr = -1;
	readPageBuf.EmptyItOut();
	//currentPage = 0;
	//myFile.GetPage(&readPageBuf, currentPage);
}

int Heap::Close() {
	char *metaFile = new char[100];
	sprintf(metaFile, "%s.meta", fileName);
	FILE *meta = fopen(metaFile, "w");
	if (!meta) {
		cerr << "Open meta file error!" << endl;
		return 0;
	}
	fprintf(meta, "%d\n", 1);
	fclose(meta);
	delete[] metaFile;
	myFile.Close();
	return 1;
}

void Heap::Add(Record &rec) {
	Record temp;
	temp.Consume(&rec);
	if (!mode) {
		// if writePageBuf is empty, it was either written to file during a read operation or this is first Add operation.
		if (myFile.GetLength() >= 2) {
			// if this is not first Add operation, first get last page of file.
			myFile.GetPage(&writePageBuf, writePageCtr);
		}
		mode = 1;
	}
	// Add page to file if buffer is full.
	if (!writePageBuf.Append(&temp)) {
		myFile.AddPage(&writePageBuf, writePageCtr);
		if (readPageCtr == writePageCtr) {
			readBufOutOfSync = true;
		}
		cout << "Added to File" << endl;
		writePageBuf.EmptyItOut();
		writePageBuf.Append(&temp);
		writePageCtr++;
	}
}

int Heap::GetNext(Record &fetchme) {
	if (mode) {
		myFile.AddPage(&writePageBuf, writePageCtr);
		writePageBuf.EmptyItOut();
		if (readPageCtr == writePageCtr) {
			readBufOutOfSync = true;
		}
		mode = 0;
	}
	// read next record from page buffer.
	if (!readPageBuf.GetFirst(&fetchme)) {
		// if readBuf is not out of sync, check if current page is not last page, fetch next page.
		if (readPageCtr < myFile.GetLength() - 2) {
			++readPageCtr;
			myFile.GetPage(&readPageBuf, readPageCtr);
			if (readBufOutOfSync) {
				readBufOutOfSync = false;
			} else {
				readCtr = 0;
			}
			// if read buffer is out of sync, get the page again and fetch record.
			if (readCtr < readPageBuf.GetLength()) {
				int i = 0;
				while (i < readCtr) {
					readPageBuf.GetFirst(&fetchme);
					i++;
					cout << "Read 1 : " << readCtr << endl;
				}
			}
			readPageBuf.GetFirst(&fetchme);
			readCtr++;
			cout << "Read 2 : " << readCtr << endl;
			return 1;
		} else {
			return 0;
		}
	}
// increment record counter in current buffer.
	readCtr++;
	cout << "Read 3 : " << readCtr << endl;
	return 1;
}

int Heap::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
// loop until we break; we break when a record is matched.
	while (1) {
		// read next record from page buffer.
		if (!readPageBuf.GetFirst(&fetchme)) {
			// if read page buffer is empty.
			++readPageCtr;
			if (readPageCtr + 1 < myFile.GetLength()) {
				// get next page if available and read the record from there.
				myFile.GetPage(&readPageBuf, readPageCtr);
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
