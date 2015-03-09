#ifndef DBFILE_H
#define DBFILE_H
class OrderMaker;

class CNF;
class GenericDBFile;
class Record;
class Schema;

typedef enum {
	sorted, heap, tree, err
} fType;

class DBFile {
private:
	GenericDBFile *internalVar;
public:

	DBFile();
	~DBFile();

	int Create(char *fpath, fType file_type, void *startup);
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
