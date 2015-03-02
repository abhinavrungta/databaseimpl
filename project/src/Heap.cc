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
	currentPage = -1;
	return 1;
}

void Heap::MoveFirst() {
	currentPage = -1;
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
	if (!writePageBuf.GetLength()) {
		// if writePageBuf is empty, it was either written to file during a read operation or this is first Add operation.
		if (myFile.GetLength() >= 2) {
			// if this is not first Add operation, first get last page of file.
			myFile.GetPage(&writePageBuf, myFile.GetLength() - 2);
			if (currentPage == myFile.GetLength() - 2) {
				// if currentPage(current Read Page) is the same as this, set flag that readBuffer is out of sync.
				readBufOutOfSync = true;
			}
		}
	}
	int pos = myFile.GetLength() == 0 ? 0 : myFile.GetLength() - 2;
	// Add page to file if buffer is full.
	if (!writePageBuf.Append(&temp)) {
		myFile.AddPage(&writePageBuf, pos);
		writePageBuf.EmptyItOut();
		writePageBuf.Append(&temp);
		newPage = true;
	}
}

int Heap::GetNext(Record &fetchme) {
// read next record from page buffer.
	if (!readPageBuf.GetFirst(&fetchme)) {
		// if read buffer is out of sync, get the page again and fetch record.
		if (readBufOutOfSync) {
			// if it is the last page, we need to
			if ((currentPage == myFile.GetLength() - 2) && !newPage) {
				myFile.AddPage(&writePageBuf, currentPage);
				writePageBuf.EmptyItOut();
			}
			myFile.GetPage(&readPageBuf, currentPage);
			readBufOutOfSync = false;
			// if there are records in the current page which had not been read, fetch that record.
			if (readCtr < readPageBuf.GetLength()) {
				int i = 0;
				while (i < readCtr) {
					readPageBuf.GetFirst(&fetchme);
					i++;
				}
				readPageBuf.GetFirst(&fetchme);
				readCtr++;
				return 1;
			}
		}
		// if readBuf is not out of sync, check if current page is not last page, fetch next page.
		if (currentPage < myFile.GetLength() - 2) {
			++currentPage;
			myFile.GetPage(&readPageBuf, currentPage);
			readPageBuf.GetFirst(&fetchme);
			readCtr = 1;
			return 1;
		} else {
			// if last page is reached, check if there is anything in the writebuf.
			if (writePageBuf.GetLength()) {
				int pos = myFile.GetLength() == 0 ? 0 : myFile.GetLength() - 1;
				myFile.AddPage(&writePageBuf, pos);
				writePageBuf.EmptyItOut();
				newPage = false;
				++currentPage;
				// get next page if available and read the record from there.
				myFile.GetPage(&readPageBuf, currentPage);
				readPageBuf.GetFirst(&fetchme);
				readCtr = 1;
				return 1;
			}
			return 0;
		}
	}
	// increment record counter in current buffer.
	readCtr++;
	return 1;
}

int Heap::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
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
