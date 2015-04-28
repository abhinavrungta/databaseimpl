#include <iostream>
#include "Record.h"
#include <stdlib.h>
using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main(int argc, char* argv[]) {

	char* dbFilePath = argv[1];

	// try to parse the CNF
	cout << "Enter in your CNF: ";
	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit(1);
	}

	// suck up the schema from the file
	Schema lineitem("catalog", "lineitem");

	// grow the CNF expression from the parse tree
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree(final, &lineitem, literal);

	// print out the comparison to the screen
	myComparison.Print();

	// now open up the text file and start procesing it
	FILE *tableFile = fopen(strcat(dbFilePath, "/lineitem.tbl"), "r");

	Record temp;
	// read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0;
	ComparisonEngine comp;
	while (temp.SuckNextRecord(&lineitem, tableFile) == 1) {
		counter++;
		if (comp.Compare(&temp, &literal, &myComparison))
			temp.Print(&lineitem);
	}
	cout << counter << " Records found";
}
