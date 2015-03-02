#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H

#include "File.h"

class GenericDBFile {
protected:
	File myFile;
	Page writePageBuf;
	off_t currentPage;
	Page readPageBuf;
	int readCtr;
	int mode;	// 0 for read, 1 for write.
	bool readBufOutOfSync;
	bool newPage;

public:
	char* fileName;
	GenericDBFile() {
		fileName = new char[100];
		currentPage = 0;
		readCtr = 0;
		mode = 0;
		readBufOutOfSync = false;
		newPage = false;
	}
	virtual ~GenericDBFile() {
		delete[] fileName;
	}

	virtual int Create(char *fpath, void *startup) = 0;
	virtual int Open(char *fpath) = 0;
	virtual int Close() = 0;

	virtual void Load(Schema &myschema, char *loadpath) = 0;

	virtual void MoveFirst() = 0;
	virtual void Add(Record &addme) = 0;
	virtual int GetNext(Record &fetchme) = 0;
	virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;

};
#endif
