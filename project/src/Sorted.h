#ifndef SORTED_H
#define SORTED_H

#include "GenericDBFile.h"
#include "BigQ.h"

struct SortInfo {
	OrderMaker *myOrder;
	int runLength;
};

class Sorted: public GenericDBFile {
	SortInfo *info;
	BigQ *bigQ;
	Pipe *input, *output;
	Page mergePageBuf;
	int mergePageCtr;
	File tmpFile;
	bool queryChanged;
	OrderMaker *queryOrder;
	OrderMaker *literalOrder;

	void MergeBigQ();
	void addToFile(Record &temp);
	int BinarySearch(int low, int high, OrderMaker *order, OrderMaker *lorder,
			Record &literal);

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
	OrderMaker* GetSortOrder();

};
#endif
