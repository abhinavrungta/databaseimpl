#include "DBFile.h"
#include "TestBase.h"

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

// The fixture for testing class DBFile.
class DBFileTest: public BaseTest {
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

	// Objects declared here can be used by all tests in the test case for DBFile.
	char *dbfile_dir, *tpch_dir, *catalog_path; // full path of the catalog file
	relation *rel;

	DBFileTest() {
		// You can do set-up work for each test here.
		dbfile_dir = getenv("dbfile"); // dir where binary heap files should be stored
		tpch_dir = getenv("tpch"); // dir where dbgen tpch files (extension *.tbl) can be found
		catalog_path = "catalog"; // full path of the catalog file
	}

	virtual ~DBFileTest() {
		// You can do clean-up work that doesn't throw exceptions here.
	}

	// If the constructor and destructor are not enough for setting up
	// and cleaning up each test, you can define the following methods:
	virtual void SetUp() {
		char *supplier = "supplier";
		char *partsupp = "partsupp";
		char *part = "part";
		char *nation = "nation";
		char *customer = "customer";
		char *orders = "orders";
		char *region = "region";
		char *lineitem = "lineitem";

		char* rel_ptr[] = { nation, region, customer, part, partsupp, orders,
				lineitem };
		cout
				<< " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
		cout << " catalog location: \t" << catalog_path << endl;
		cout << " tpch files dir: \t" << tpch_dir << endl;
		cout << " heap files dir: \t" << dbfile_dir << endl;
		cout << " \n\n";

		int findx = 7;
//		findx = 0;
//		while (findx < 1 || findx > 7) {
//			cout << "\n select table: \n";
//			cout << "\t 1. nation \n";
//			cout << "\t 2. region \n";
//			cout << "\t 3. customer \n";
//			cout << "\t 4. part \n";
//			cout << "\t 5. partsupp \n";
//			cout << "\t 6. orders \n";
//			cout << "\t 7. lineitem \n \t ";
//			cin >> findx;
//		}

		char* reltype = rel_ptr[findx - 1];
		rel = new relation(reltype, new Schema(catalog_path, reltype),
				dbfile_dir);
	}

	virtual void TearDown() {
	}

};

// load dbfile from raw file.
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
		//temp.Print(rel->schema());
	}
	cout << " scanned " << counter << " recs \n";
	dbfile.Close();
}

// MoveFirst works and by default record points to first.
TEST_F(DBFileTest, MoveFirst) {

	Record rec1, rec2;

	DBFile dbfile;
	dbfile.Open(rel->path());
	dbfile.GetNext(rec1);
	dbfile.MoveFirst();
	dbfile.GetNext(rec2);

	ASSERT_TRUE(RecordCompare(&rec1, &rec2));
	dbfile.Close();
}

// Add record, verify it is consumed, verify it is added at last, verify number of records.
TEST_F(DBFileTest, Add) {
	DBFile dbfile;
	Record rec1, tmp, rec2;
	dbfile.Open(rel->path());
	int ctr1 = 0;
	while (dbfile.GetNext(tmp) == 1) {
		ctr1 += 1;
	}

	dbfile.MoveFirst();
	dbfile.GetNext(tmp);
	rec1.Copy(&tmp);
	dbfile.Add(tmp);
	dbfile.MoveFirst();
	ASSERT_TRUE(GetRecordBits(&tmp)==NULL);
	int ctr2 = 0;
	while (dbfile.GetNext(rec2) == 1) {
		ctr2 += 1;
	}
	dbfile.Close();
	ASSERT_TRUE(RecordCompare(&rec1, &rec2));
	ASSERT_EQ(ctr2, ctr1 + 1);
}

// verify that records added in same order as tpch file for heap filesystem.
TEST_F(DBFileTest, Next) {
	DBFile dbfile;
	dbfile.Open(rel->path());

	Schema lineitem("catalog", "lineitem");
	// grow the CNF expression from the parse tree
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree(final, &lineitem, literal);

	char tbl_path[100]; // construct path of the tpch flat text file
	sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
	// now open up the text file and start procesing it
	FILE *tableFile = fopen(tbl_path, "r");

	Record raw;
	while (raw.SuckNextRecord(&lineitem, tableFile) == 1) {
		Record db;
		dbfile.GetNext(db);
		ASSERT_TRUE(RecordCompare(&raw, &db));
	}
	dbfile.Close();
}

// verify that records added in same order as tpch file for heap filesystem, after filtering based on cnf
TEST_F(DBFileTest, NextCnf) {
	DBFile dbfile;

	dbfile.Open(rel->path());
	char tbl_path[100]; // construct path of the tpch flat text file
	sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
	cout << " tpch file will be loaded from " << tbl_path << endl;
	dbfile.Load(*(rel->schema()), tbl_path);
	dbfile.Close();

	dbfile.Open(rel->path());
	CNF cnf;
	Record literal, db, raw;
	Schema* schema = rel->schema();
	ComparisonEngine comp;
	rel->get_cnf(cnf, literal);

	// now open up the text file and start procesing it
	FILE *tableFile = fopen(tbl_path, "r");
	int ctr = 0;
	while (raw.SuckNextRecord(schema, tableFile) == 1) {
		if (comp.Compare(&raw, &literal, &cnf)) {
			ctr++;
			dbfile.GetNext(db, cnf, literal);
			ASSERT_TRUE(RecordCompare(&raw, &db));
		}
	}
	ASSERT_FALSE(dbfile.GetNext(db, cnf, literal));
	dbfile.Close();
}
