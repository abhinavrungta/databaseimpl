#include "GenericDBFile.h"

class Heap: public GenericDBFile {

public:
	Heap();
	~Heap();

	int Create(char *fpath, void *startup);
	int Open(char *fpath);
	int Close();

	void Load(Schema &myschema, char *loadpath);

	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);

};
