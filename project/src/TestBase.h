#include <iostream>
#include "Record.h"
#include "gtest/gtest.h"

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

// The fixture for testing class DBFile.
class BaseTest: public testing::Test {
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
		void get_sort_order(OrderMaker &sortorder) {
			cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
			if (yyparse() != 0) {
				cout << "Can't parse your sort CNF.\n";
				exit(1);
			}
			cout << " \n";
			Record literal;
			CNF sort_pred;
			sort_pred.GrowFromParseTree(final, schema(), literal); // constructs CNF predicate
			OrderMaker dummy;
			sort_pred.GetSortOrders(sortorder, dummy);
		}
		~relation() {
			delete rschema;
		}
	};

	// Objects declared here can be used by all tests in the test case for DBFile.
	char *dbfile_dir, *tpch_dir, *catalog_path; // full path of the catalog file
	relation *rel;

	BaseTest() {
		// You can do set-up work for each test here.
		// You can do set-up work for each test here.
		dbfile_dir = getenv("dbfile"); // dir where binary heap files should be stored
		tpch_dir = getenv("tpch"); // dir where dbgen tpch files (extension *.tbl) can be found
		catalog_path = "catalog"; // full path of the catalog file
	}

	virtual ~BaseTest() {
		// You can do clean-up work that doesn't throw exceptions here.
		delete rel;
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

		int findx = 0;
		while (findx < 1 || findx > 7) {
			cout << "\n select table: \n";
			cout << "\t 1. nation \n";
			cout << "\t 2. region \n";
			cout << "\t 3. customer \n";
			cout << "\t 4. part \n";
			cout << "\t 5. partsupp \n";
			cout << "\t 6. orders \n";
			cout << "\t 7. lineitem \n \t ";
			cin >> findx;
		}

		char* reltype = rel_ptr[findx - 1];
		rel = new relation(reltype, new Schema(catalog_path, reltype),
				dbfile_dir);
	}

	virtual void TearDown() {
	}

	bool RecordCompare(Record *a, Record *b) {
		if (strcmp(a->GetBits(), b->GetBits()) == 0) {
			return true;
		}
		return false;
	}

	char* GetRecordBits(Record *a) {
		return a->GetBits();
	}
};
