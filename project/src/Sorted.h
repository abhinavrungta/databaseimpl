#ifndef SORTED_H
#define SORTED_H

#include "GenericDBFile.h"

struct SortInfo {
	OrderMaker *myOrder;
	int runLength;
};

class Sorted: public GenericDBFile {
	SortInfo *info;
public:
	Sorted();
	~Sorted();

	int Create(char *fpath, void *startup);
	int Open(char *fpath);
	int Close();

	void Load(Schema &myschema, char *loadpath);

	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);

};
#endif
