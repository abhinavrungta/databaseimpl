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
	void Query(relation *rel);
};

int SortedDBFileTest::add_data(FILE *src, int numrecs, int &res,
		relation *rel) {
	DBFile dbfile;
	dbfile.Open(rel->path());
	Record temp;

	int proc = 0;
	int xx = 20000;
	while (proc < numrecs && (res = temp.SuckNextRecord(rel->schema(), src))) {
		dbfile.Add(temp);
		proc++;
		if (proc == xx)
			cerr << "\t ";
		if (proc % xx == 0)
			cerr << ".";
	}
	dbfile.Close();
	return proc;
}

void SortedDBFileTest::Query(relation *rel) {
	CNF cnf;
	Record literal;
	rel->get_cnf(cnf, literal);

	DBFile dbfile;
	dbfile.Open(rel->path());
	dbfile.MoveFirst();

	int err = 0;
	int i = 0;
	Record rec[2];
	Record *last = NULL, *prev = NULL;
	ComparisonEngine ceng;

	while (dbfile.GetNext(rec[i % 2], cnf, literal)) {
		prev = last;
		last = &rec[i % 2];
		if (prev && last) {
			if (ceng.Compare(prev, last, dbfile.GetSortOrder()) > 0) {
				err++;
			}
		}
		i++;
	}
	EXPECT_EQ(0, err);
	cout << "\n query over " << rel->path() << " returned " << i << " recs\n";
	dbfile.Close();
}

// create a dbfile interactively
TEST_F(SortedDBFileTest, Add) {
	int runlen = 0;
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
		int x = 0;
		while (x < 1 || x > 3) {
			cout << "\n select option for : " << rel->path() << endl;
			cout << " \t 1. add a few (1 to 1k recs)\n";
			cout << " \t 2. add a lot (1k to 1e+06 recs) \n";
			cout << " \t 3. run some query \n \t ";
			cin >> x;
		}
		if (x < 3) {
			proc = add_data(tblfile,
					lrand48() % (int) pow(1e3, x) + (x - 1) * 1000, res, rel);
			tot += proc;
			if (proc)
				cout << "\n\t added " << proc << " recs..so far " << tot
						<< endl;
		} else {
			Query(rel);
		}
	}
	cout << "\n create finished.. " << tot << " recs inserted\n";
	fclose(tblfile);
}

// sequential scan of a DBfile
TEST_F(SortedDBFileTest, Scan) {
	DBFile dbfile;
	dbfile.Open(rel->path());
	dbfile.MoveFirst();

	int err = 0;
	int i = 0;
	Record rec[2];
	Record *last = NULL, *prev = NULL;
	ComparisonEngine ceng;

	while (dbfile.GetNext(rec[i % 2])) {
		prev = last;
		last = &rec[i % 2];
		if (prev && last) {
			if (ceng.Compare(prev, last, dbfile.GetSortOrder()) > 0) {
				err++;
			}
		}
		i++;
	}
	EXPECT_EQ(0, err);
	cout << "\n scanned " << i << " recs \n";
	dbfile.Close();
}

TEST_F(SortedDBFileTest, Query) {
	Query(rel);
}
