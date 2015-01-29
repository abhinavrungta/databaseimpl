#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <string.h>
#include <iostream>

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile() {
	currentPage = 0;
	myFile.GetPage(&readPageBuf, currentPage);
}

int DBFile::Create(char *f_path, fType f_type, void *startup) {
	myFile.Open(0, f_path);
	std::string s = f_path;
	int i = s.find_last_of('/');
	std::string s2 = s.substr(i + 1);

	struct kv meta;
	meta.name = s2.c_str();
	meta.type = f_type;

	FILE *pFile = fopen("tmp", "a");
	off_t size = sizeof(meta);
	std::string s3 = std::to_string(size);
	fputs(s3.c_str(), pFile);
	fwrite(&meta, sizeof(struct kv), 1, pFile);
	fclose(pFile);
	myFile.Close();
	return 1;
}

fType DBFile::getFileMetaData(char* fName) {
	FILE *pFile = fopen("/tmp", "r");
	char* buffer;
	off_t recSize;
	struct kv temp;
	if (pFile == NULL)
		perror("Error opening file");
	else {
		while (!feof(pFile)) {
			if (fgets(buffer, sizeof(off_t), pFile) != NULL) {
				recSize = *((off_t*) buffer);
				buffer = NULL;
				if (fgets(buffer, recSize, pFile) != NULL) {
					temp = *((struct kv*) buffer);
					if (*(temp.name) == *fName) {
						fclose(pFile);
						return temp.type;
					}
				} else {
					break;
				}
			} else {
				break;
			}
		}
		fclose(pFile);
		return err;
	}
}

void DBFile::Load(Schema &f_schema, char *loadpath) {
	// now open up the text file and start procesing it
	FILE *tableFile = fopen(loadpath, "r");
	Record temp;
	Page p;
	// read in all of the records from the text file.
	int counter = 0, i = 0;
	while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}
		if (!p.Append(&temp)) {
			myFile.AddPage(&p, i++);
			p.EmptyItOut();
			p.Append(&temp);
		}
	}
	myFile.AddPage(&p, i++);
	p.EmptyItOut();
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
