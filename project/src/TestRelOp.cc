#include <iostream>

#include "../gtest/gtest.h"
#include "Comparison.h"
#include "DBFile.h"
#include "Defs.h"
#include "Function.h"
#include "Pipe.h"
#include "Record.h"
#include "RelOp.h"
#include "Schema.h"
#include "TestBase.h"

Attribute IA = { "int", Int };
Attribute SA = { "string", String };
Attribute DA = { "double", Double };

typedef struct {
	Pipe *pipe;
	OrderMaker *order;
	bool print;
	bool write;
} testutil;

// The fixture for testing class DBFile.
class RelOpTest: public BaseTest {
protected:
	int pipesz = 100; // buffer sz allowed for each pipe
	int buffsz = 100;
	Pipe *pip = new Pipe(pipesz);
	SelectFile sf;
	DBFile db;
	CNF cnf;
	Record rec;
	Function func;

	int pAtts = 9;
	int psAtts = 5;
	int liAtts = 16;
	int oAtts = 9;
	int sAtts = 7;
	int cAtts = 8;
	int nAtts = 4;
	int rAtts = 3;

	RelOpTest() {
	}

	virtual ~RelOpTest() {
		// You can do clean-up work that doesn't throw exceptions here.
	}
	int clear_pipe(Pipe &in_pipe, Schema *schema, bool print);
	int clear_pipe(Pipe &in_pipe, Schema *schema, Function &func, bool print);
	void init_SF(char *pred_str, int numpgs);
};
int RelOpTest::clear_pipe(Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove(&rec)) {
		if (print) {
			rec.Print(schema);
		}
		cnt++;
	}
	return cnt;
}

int RelOpTest::clear_pipe(Pipe &in_pipe, Schema *schema, Function &func,
		bool print) {
	Record rec;
	int cnt = 0;
	double sum = 0;
	while (in_pipe.Remove(&rec)) {
		if (print) {
			rec.Print(schema);
		}
		int ival = 0;
		double dval = 0;
		func.Apply(rec, ival, dval);
		sum += (ival + dval);
		cnt++;
	}
	cout << " Sum: " << sum << endl;
	return cnt;
}

void RelOpTest::init_SF(char *pred_str, int numpgs) {
	db.Open(rel->path());
	rel->get_cnf(pred_str, rel->schema(), cnf, rec);
	sf.Use_n_Pages(numpgs);
}

// create a dbfile interactively
TEST_F(RelOpTest, q1) {
	char *pred_ps = "(ps_supplycost < 1.03)";
	init_SF(pred_ps, 100);

	sf.Run(db, *pip, cnf, rec);
	sf.WaitUntilDone();

	int cnt = clear_pipe(*pip, rel->schema(), true);
	cout << "\n\n query1 returned " << cnt << " records \n";

	db.Close();
}

// sequential scan of a DBfile
TEST_F(RelOpTest, q2) {
	char *pred_p = "(p_retailprice > 931.01) AND (p_retailprice < 931.3)";
	init_SF(pred_p, 100);

	Project P_p;
	Pipe _out(pipesz);
	int keepMe[] = { 0, 1, 7 };
	int numAttsIn = pAtts;
	int numAttsOut = 3;
	P_p.Use_n_Pages(buffsz);

	sf.Run(db, *pip, cnf, rec);
	P_p.Run(*pip, _out, keepMe, numAttsIn, numAttsOut);

	sf.WaitUntilDone();
	P_p.WaitUntilDone();

	Attribute att3[] = { IA, SA, DA };
	Schema out_sch("out_sch", numAttsOut, att3);
	int cnt = clear_pipe(*pip, rel->schema(), true);

	cout << "\n\n query2 returned " << cnt << " records \n";

	db.Close();
}

TEST_F(RelOpTest, Query) {
}
