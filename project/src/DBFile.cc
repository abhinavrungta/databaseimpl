#include "DBFile.h"

#include <string.h>
#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "Heap.h"
#include "Sorted.h"

DBFile::DBFile() {
	internalVar = NULL;
}

DBFile::~DBFile() {
	delete internalVar;
	internalVar = NULL;
}

// internalVar is an instance of GenericDBFile class. Based on F_type create an object and close the file.
int DBFile::Create(char *f_path, fType f_type, void *startup) {
	if (f_type == heap) {
		internalVar = new Heap();
	} else if (f_type == sorted) {
		internalVar = new Sorted();
	}
	if (internalVar != NULL) {
		strcpy(internalVar->fileName, f_path);
	}
	internalVar->Create(f_path, startup);
	return internalVar->Close();
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
	// read file type from meta file, which was created when file was previously closed. Then create object accordingly.
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
	// store fileName of file opened.
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

OrderMaker* DBFile::GetSortOrder() {
	return internalVar->GetSortOrder();
}
