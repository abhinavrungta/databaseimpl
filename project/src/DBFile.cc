#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile() {
	currentPage = 0;
	myFile.GetPage(&readPageBuf, currentPage);
}

int DBFile::Create(char *f_path, fType f_type, void *startup) {
	myFile.Open(0, f_path);
	FILE *metaFile = fopen("/tmp", "rw");
	struct kv {
		char* name;
		fType type;
		kv* next;
	} meta;
}

void DBFile::Load(Schema &f_schema, char *loadpath) {
}

int DBFile::Open(char *f_path) {
	myFile.Open(1, f_path);
	return 1;
}

void DBFile::MoveFirst() {
	currentPage = 0;
	myFile.GetPage(&readPageBuf, currentPage);
}

int DBFile::Close() {
	if (writePageBuf.GetLength()) {
		myFile.AddPage(&writePageBuf, myFile.GetLength());
		writePageBuf.EmptyItOut();
	}
	if (myFile.Close()) {
		return 1;
	}
	return 0;
}

void DBFile::Add(Record &rec) {
	Record temp;
	temp.Consume(&rec);
	if (!writePageBuf.Append(&temp)) {
		myFile.AddPage(&writePageBuf, myFile.GetLength());
		writePageBuf.EmptyItOut();
		writePageBuf.Append(&temp);
	}
}

int DBFile::GetNext(Record &fetchme) {
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

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
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
