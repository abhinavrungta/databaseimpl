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
	void init_SF(char *pred_str, int numpgs, DBFile &db, CNF &cnf, Record &rec,
			SelectFile &sf, relation *rel);
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

void RelOpTest::init_SF(char *pred_str, int numpgs, DBFile &db, CNF &cnf,
		Record &rec, SelectFile &sf, relation *rela) {
	db.Open(rela->path());
	rela->get_cnf(pred_str, rela->schema(), cnf, rec);
	sf.Use_n_Pages(numpgs);
}

// create a dbfile interactively
TEST_F(RelOpTest, q1) {
	char *pred_ps = "(ps_supplycost < 103.00)";
	DBFile db;
	CNF cnf;
	Record rec;
	SelectFile sf;
	init_SF(pred_ps, 100, db, cnf, rec, sf, rel);

	Pipe *pip = new Pipe(pipesz);
	sf.Run(db, *pip, cnf, rec);

	int cnt = clear_pipe(*pip, rel->schema(), true);
	sf.WaitUntilDone();

	cout << "\n\n query1 returned " << cnt << " records \n";

	db.Close();
}

// sequential scan of a DBfile
TEST_F(RelOpTest, q2) {
	char *pred_p = "(p_retailprice > 931.01) AND (p_retailprice < 931.3)";
	SelectFile sf;
	DBFile db;
	CNF cnf;
	Record rec;
	init_SF(pred_p, 100, db, cnf, rec, sf, rel);

	Project P_p;
	Pipe _out(pipesz);
	int keepMe[] = { 0, 1, 7 };
	int numAttsIn = pAtts;
	int numAttsOut = 3;
	P_p.Use_n_Pages(buffsz);

	Pipe *pip = new Pipe(pipesz);
	sf.Run(db, *pip, cnf, rec);
	P_p.Run(*pip, _out, keepMe, numAttsIn, numAttsOut);

	sf.WaitUntilDone();
	P_p.WaitUntilDone();

	Attribute att3[] = { IA, SA, DA };
	Schema out_sch("out_sch", numAttsOut, att3);
	int cnt = clear_pipe(_out, &out_sch, true);

	cout << "\n\n query2 returned " << cnt << " records \n";

	db.Close();
}

TEST_F(RelOpTest, q3) {
	char *pred_s = "(s_suppkey = s_suppkey)";
	SelectFile sf;
	DBFile db;
	CNF cnf;
	Record rec;
	relation *rel_s = new relation("supplier",
			new Schema(catalog_path, "supplier"), dbfile_dir);
	init_SF(pred_s, 100, db, cnf, rec, sf, rel_s);
	Pipe *pip = new Pipe(pipesz);
	sf.Run(db, *pip, cnf, rec);

	char *pred_ps = "(ps_suppkey = ps_suppkey)";
	SelectFile sf2;
	DBFile db2;
	CNF cnf2;
	Record rec2;
	relation *rel_ps = new relation("partsupp",
			new Schema(catalog_path, "partsupp"), dbfile_dir);
	init_SF(pred_ps, 100, db2, cnf2, rec2, sf2, rel_ps);
	Pipe *pip2 = new Pipe(pipesz);
	sf2.Run(db2, *pip2, cnf2, rec2);

	Join J;
	// left sf
	// right sf2
	Pipe _s_ps(pipesz);
	CNF cnf_p_ps;
	Record lit_p_ps;
	rel->get_cnf("(s_suppkey = ps_suppkey)", rel_s->schema(), rel_ps->schema(),
			cnf_p_ps, lit_p_ps);

	int outAtts = sAtts + psAtts;
	Attribute ps_supplycost = { "ps_supplycost", Double };
	Attribute joinatt[] = { IA, SA, SA, IA, SA, DA, SA, IA, IA, IA,
			ps_supplycost, SA };
	Schema join_sch("join_sch", outAtts, joinatt);

	J.Run(*pip, *pip2, _s_ps, cnf_p_ps, lit_p_ps);

	Schema sum_sch("sum_sch", 1, &DA);
	int cnt = clear_pipe(_s_ps, &sum_sch, false);
	sf.WaitUntilDone();
	sf2.WaitUntilDone();
	J.WaitUntilDone();
	cout << " query4 returned " << cnt << " recs \n";

}
