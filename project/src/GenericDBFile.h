#include "File.h"

class GenericDBFile {
private:
	char* fileName;
	File myFile;
	Page writePageBuf;
	off_t currentPage;
	Page readPageBuf;

public:

	GenericDBFile() {
		fileName = new char[100];
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
