#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H

#include "File.h"

class GenericDBFile {
protected:
	File myFile;
	Page writePageBuf;
	int writePageCtr;
	Page readPageBuf;
	off_t readPageCtr;
	int readCtr;		// for currentRecord Number in the readBuf.
	int mode;	// 0 for read, 1 for write.

public:
	char* fileName;
	GenericDBFile() {
		fileName = new char[100];
		readPageCtr = 0;
		writePageCtr = 0;
		readCtr = 0;
		mode = 0;
	}
	virtual ~GenericDBFile() {
		delete[] fileName;
	}

	// abstract functions defined in subclasses.
	virtual int Create(char *fpath, void *startup) = 0;
	virtual int Open(char *fpath) = 0;
	virtual int Close() = 0;
	virtual void Load(Schema &myschema, char *loadpath) = 0;
	virtual void MoveFirst() = 0;
	virtual void Add(Record &addme) = 0;
	virtual int GetNext(Record &fetchme) = 0;
	virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
	virtual OrderMaker* GetSortOrder() = 0;

};
#endif
