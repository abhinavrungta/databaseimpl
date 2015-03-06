#include <math.h>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>

#include "../gtest/gtest.h"
#include "Comparison.h"
#include "DBFile.h"
#include "Record.h"
#include "TestBase.h"

// The fixture for testing class DBFile.
class SortedDBFileTest: public BaseTest {
protected:

	SortedDBFileTest() {
	}

	virtual ~SortedDBFileTest() {
		// You can do clean-up work that doesn't throw exceptions here.
	}

	int add_data(FILE *src, int numrecs, int &res, relation *rel);
};

int SortedDBFileTest::add_data(FILE *src, int numrecs, int &res,
		relation *rel) {
	DBFile dbfile;
	dbfile.Open(rel->path());
	Record temp;

	int proc = 0;
	int xx = 20000;
	while ((res = temp.SuckNextRecord(rel->schema(), src)) && ++proc < numrecs) {
		dbfile.Add(temp);
		if (proc == xx)
			cerr << "\t ";
		if (proc % xx == 0)
			cerr << ".";
	}
	dbfile.Close();
	return proc;
}

// create a dbfile interactively
TEST_F(SortedDBFileTest, Add) {
	int runlen = 10;
	while (runlen < 1) {
		cout << "\t\n specify runlength:\n\t ";
		cin >> runlen;
	}

	OrderMaker o;
	rel->get_sort_order(o);

	struct SortInfo {
		OrderMaker *o;
		int l;
	};
	SortInfo *startup = new SortInfo();
	startup->o = &o;
	startup->l = runlen;

	DBFile dbfile;
	dbfile.Create(rel->path(), sorted, startup);

	char tbl_path[100];
	sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());

	FILE *tblfile = fopen(tbl_path, "r");
	srand48(time(NULL));

	int proc = 1, res = 1, tot = 0;
	while (proc && res) {
		int x = 1;
		while (x < 1 || x > 3) {
			cout << "\n select option for : " << rel->path() << endl;
			cout << " \t 1. add a few (1 to 1k recs)\n";
			cout << " \t 2. add a lot (1k to 1e+06 recs) \n";
			cin >> x;
		}
		proc = add_data(tblfile, lrand48() % (int) pow(1e3, x) + (x - 1) * 1000,
				res, rel);
		tot += proc;
		if (proc)
			cout << "\n\t added " << proc << " recs..so far " << tot << endl;
	}
	cout << "\n create finished.. " << tot << " recs inserted\n";
	fclose(tblfile);
}

// sequential scan of a DBfile
TEST_F(SortedDBFileTest, Scan) {
	DBFile dbfile;
	dbfile.Open(rel->path());
	dbfile.MoveFirst();

	Record temp;

	int cnt = 0;
	while (dbfile.GetNext(temp) && ++cnt) {
		temp.Print(rel->schema());
		if (cnt % 10000) {
			cerr << ".";
		}
	}
	cout << "\n scanned " << cnt << " recs \n";
	dbfile.Close();
}

TEST_F(SortedDBFileTest, Query) {
	CNF cnf;
	Record literal;
	rel->get_cnf(cnf, literal);

	DBFile dbfile;
	dbfile.Open(rel->path());
	dbfile.MoveFirst();

	Record temp;

	int cnt = 0;
	while (dbfile.GetNext(temp, cnf, literal)) {
		++cnt;
		temp.Print(rel->schema());
		if (cnt % 10000 == 0) {
			cerr << ".";
		}
	}
	cout << "\n query over " << rel->path() << " returned " << cnt << " recs\n";
	dbfile.Close();

}
