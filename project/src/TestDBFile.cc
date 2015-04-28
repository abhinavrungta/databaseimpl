#include "DBFile.h"
#include "TestBase.h"

// The fixture for testing class DBFile.
class DBFileTest: public BaseTest {
protected:

	DBFileTest() {
	}

	virtual ~DBFileTest() {
		// You can do clean-up work that doesn't throw exceptions here.
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

	// grow the CNF expression from the parse tree
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree(boolean, rel->schema(), literal);

	char tbl_path[100]; // construct path of the tpch flat text file
	sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
	// now open up the text file and start procesing it
	FILE *tableFile = fopen(tbl_path, "r");

	Record raw;
	while (raw.SuckNextRecord(rel->schema(), tableFile) == 1) {
		Record db;
		dbfile.GetNext(db);
		ASSERT_TRUE(RecordCompare(&raw, &db));
	}
	dbfile.Close();
}

// verify that records added in same order as tpch file for heap filesystem, after filtering based on cnf
TEST_F(DBFileTest, NextCnf) {
	DBFile dbfile;
	dbfile.Create(rel->path(), heap, NULL);
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
