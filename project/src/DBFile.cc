#include "DBFile.h"

#include <string.h>

#include "Heap.h"
#include "Sorted.h"

DBFile::DBFile() {
	internalVar = NULL;
}

DBFile::~DBFile() {
	delete internalVar;
	internalVar = NULL;
}

int DBFile::Create(char *f_path, fType f_type, void *startup) {
	if (f_type == heap) {
		internalVar = new Heap();
	} else if (f_type == sorted) {
		internalVar = new Sorted();
	}
	return internalVar->Create(f_path, startup);
}

void DBFile::Load(Schema &f_schema, char *loadpath) {
	internalVar->Load(f_schema, loadpath);
}

int DBFile::Open(char *f_path) {
	//this function assumes that the DBFile exists and has previously been created and closed
	char *headerName = new char[100];
	sprintf(headerName, "%s.meta", f_path);
	FILE *fp = fopen(headerName, "r");
	if (!fp) {
		cout << f_path << " DBFile does not exist" << endl;
		exit(-1);
	}

	fType ftype;
	fscanf(fp, "%d", &ftype);
	if (ftype == heap) {
		internalVar = new Heap();
	} else if (ftype == sorted) {
		internalVar = new Sorted();
	} else {
		cerr << "Unsupported DBFile type!" << endl;
		return 0;
	}
	fclose(fp);
	if (internalVar != NULL) {
		strcpy(internalVar->fileName, f_path);
		return internalVar->Open(f_path);
	}
	return 0;
}

void DBFile::MoveFirst() {
	internalVar->MoveFirst();
}

int DBFile::Close() {
	return internalVar->Close();
}

void DBFile::Add(Record &rec) {
	internalVar->Add(rec);
}

int DBFile::GetNext(Record &fetchme) {
	return internalVar->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	return internalVar->GetNext(fetchme, cnf, literal);
}
