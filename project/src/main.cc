#include <iostream>
#include "Record.h"
#include <stdlib.h>
using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

// int main(int argc, char* argv[]) {
//
//	char* dbFilePath = argv[1];
//
//	// try to parse the CNF
//	cout << "Enter in your CNF: ";
//	if (yyparse() != 0) {
//		cout << "Can't parse your CNF.\n";
//		exit(1);
//	}
//
//	// suck up the schema from the file
//	Schema lineitem("catalog", "lineitem");
//
//	// grow the CNF expression from the parse tree
//	CNF myComparison;
//	Record literal;
//	myComparison.GrowFromParseTree(final, &lineitem, literal);
//
//	// print out the comparison to the screen
//	myComparison.Print();
//
//	// now open up the text file and start procesing it
//	FILE *tableFile = fopen(strcat(dbFilePath, "/lineitem.tbl"), "r");
//
//	Record temp;
//	Schema mySchema("catalog", "lineitem");
//
//	//char *bits = literal.GetBits ();
//	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
//	//literal.Print (&supplier);
//
//	// read in all of the records from the text file and see if they match
//	// the CNF expression that was typed in
//	int counter = 0;
//	ComparisonEngine comp;
//	while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
//		counter++;
//		if (counter % 10000 == 0) {
//			cerr << counter << "\n";
//		}
//
//		if (comp.Compare(&temp, &literal, &myComparison))
//			temp.Print(&mySchema);
//
//	}
//
//}

int main(int argc, char* argv[]) {
	string dbFilePath = argv[1];

	// try to parse the CNF
	cout << "Enter in your CNF: ";
	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit(1);
	}

	// suck up the schema from the file
	Schema mySchema("catalog", "lineitem");

	// grow the CNF expression from the parse tree
	CNF myCNF;
	Record literal;
	myCNF.GrowFromParseTree(final, &mySchema, literal);

	// print out the comparison to the screen
	myCNF.Print();

	// now open up the text file and start procesing it
	FILE *tableFile = fopen((dbFilePath + "/lineitem.tbl").c_str(), "r");
	File fd;
	fd.Open(0, (dbFilePath + "/tmp.tbl").c_str());
	Record temp;
	Page p;
	// read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0, i = 0;
	ComparisonEngine comp;
	while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}
		if (!p.Append(&temp)) {
			fd.AddPage(&p, i++);
			p.EmptyItOut();
			p.Append(&temp);
		}
	}
	fd.AddPage(&p, i++);
	p.EmptyItOut();
	fd.Close();

// now open up the text file and start processing it
	fd.Open(1, (dbFilePath + "/tmp.tbl").c_str());
	cout << fd.GetLength();
	fd.GetPage(&p, 0);
	fd.Close();

// read all records from the page.
	while (1) {
		if (p.GetFirst(&temp)) {
			if (comp.Compare(&temp, &literal, &myCNF))
				temp.Print(&mySchema);
		} else {
			break;
		}
	}
}
