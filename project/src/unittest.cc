#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <limits.h>
#include "../src/DBFile.h"
#include "../src/Record.h"
#include "gtest/gtest.h"

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

// The fixture for testing class Foo.
class DBFileTest: public testing::Test {
protected:

	class relation {

	private:
		char *rname;
		char *prefix;
		char rpath[100];
		Schema *rschema;
	public:
		relation(char *_name, Schema *_schema, char *_prefix) :
				rname(_name), rschema(_schema), prefix(_prefix) {
			sprintf(rpath, "%s%s.bin", prefix, rname);
		}
		char* name() {
			return rname;
		}
		char* path() {
			return rpath;
		}
		Schema* schema() {
			return rschema;
		}
		void info() {
			cout << " relation info\n";
			cout << "\t name: " << name() << endl;
			cout << "\t path: " << path() << endl;
		}

		void get_cnf(CNF &cnf_pred, Record &literal) {
			cout << " Enter CNF predicate (when done press ctrl-D):\n\t";
			if (yyparse() != 0) {
				std::cout << "Can't parse your CNF.\n";
				exit(1);
			}
			cnf_pred.GrowFromParseTree(final, schema(), literal); // constructs CNF predicate
		}
	};

	char *supplier = "supplier";
	char *partsupp = "partsupp";
	char *part = "part";
	char *nation = "nation";
	char *customer = "customer";
	char *orders = "orders";
	char *region = "region";
	char *lineitem = "lineitem";

	//relation *s, *p, *ps, *n, *li, *r, *o, *c;

	// Objects declared here can be used by all tests in the test case for DBFile.
	// make sure that the file path/dir information below is correct
	char *dbfile_dir = getenv("dbfile"); // dir where binary heap files should be stored
	char *tpch_dir = getenv("tpch"); // dir where dbgen tpch files (extension *.tbl) can be found
	//char *tpch_dir = "/Users/abhinavrungta/gitlab/databaseimpl/db"; // dir where dbgen tpch files (extension *.tbl) can be found
	char *catalog_path = "catalog"; // full path of the catalog file
	relation *rel;
	// You can remove any or all of the following functions if its body
	// is empty.

	DBFileTest() {

		// You can do set-up work for each test here.
	}

	virtual ~DBFileTest() {
		// You can do clean-up work that doesn't throw exceptions here.
	}

	// If the constructor and destructor are not enough for setting up
	// and cleaning up each test, you can define the following methods:

	virtual void SetUp() {
		cout
				<< " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
		cout << " catalog location: \t" << catalog_path << endl;
		cout << " tpch files dir: \t" << tpch_dir << endl;
		cout << " heap files dir: \t" << dbfile_dir << endl;
		cout << " \n\n";
		rel = new relation(lineitem, new Schema(catalog_path, lineitem),
				dbfile_dir);

//		s = new relation(supplier, new Schema(catalog_path, supplier),
//				dbfile_dir);
//		ps = new relation(partsupp, new Schema(catalog_path, partsupp),
//				dbfile_dir);
//		p = new relation(part, new Schema(catalog_path, part), dbfile_dir);
//		n = new relation(nation, new Schema(catalog_path, nation), dbfile_dir);
//		li = new relation(lineitem, new Schema(catalog_path, lineitem),
//				dbfile_dir);
//		r = new relation(region, new Schema(catalog_path, region), dbfile_dir);
//		o = new relation(orders, new Schema(catalog_path, orders), dbfile_dir);
//		c = new relation(customer, new Schema(catalog_path, customer),
//				dbfile_dir);

	}

	virtual void TearDown() {
		//	delete s, p, ps, n, li, r, o, c;
	}
};

TEST_F(DBFileTest, Load) {
	DBFile dbfile;
	cout << " DBFile will be created at " << rel->path() << endl;
	EXPECT_EQ(1, dbfile.Create(rel->path(), heap, NULL));;
	EXPECT_EQ(1, dbfile.Open(rel->path()));

	char tbl_path[100]; // construct path of the tpch flat text file
	sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
	cout << " tpch file will be loaded from " << tbl_path << endl;

	dbfile.Load(*(rel->schema()), tbl_path);
	dbfile.Close();
}

// sequential scan of a DBfile 
TEST_F(DBFileTest, Scan) {

	DBFile dbfile;
	EXPECT_EQ(1, dbfile.Open(rel->path()));
	dbfile.MoveFirst();
	Record temp;

	int counter = 0;
	while (dbfile.GetNext(temp) == 1) {
		counter += 1;
		temp.Print(rel->schema());
	}
	cout << " scanned " << counter << " recs \n";
	dbfile.Close();
}

// scan of a DBfile and apply a filter predicate
TEST_F(DBFileTest, ScanF) {

	cout << " Filter with CNF for : " << rel->name() << "\n";

	CNF cnf;
	Record literal;
	rel->get_cnf(cnf, literal);

	DBFile dbfile;
	dbfile.Open(rel->path());
	dbfile.MoveFirst();

	Record temp;

	int counter = 0;
	while (dbfile.GetNext(temp, cnf, literal) == 1) {
		counter += 1;
		temp.Print(rel->schema());
	}
	cout << " selected " << counter << " recs \n";
	dbfile.Close();
}
