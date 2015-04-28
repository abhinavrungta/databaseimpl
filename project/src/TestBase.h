#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include "../gtest/gtest.h"
#include "Comparison.h"
#include "Function.h"
#include "Record.h"
#include "Schema.h"
#include "ParseTree.h"

extern "C" {
struct YY_BUFFER_STATE *yy_scan_string(const char*);
int yyparse(void);   // defined in y.tab.c
void init_lexical_parser(char *); // defined in lex.yy.c (from Lexer.l)
void close_lexical_parser(); // defined in lex.yy.c
}

extern struct AndList *boolean;
extern struct FuncOperator *finalFunction;
extern FILE *yyin;

// The fixture for testing class DBFile.
class BaseTest: public testing::Test {
public:
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
		void setPath(char *path) {
			sprintf(rpath, "%s", path);
		}
		void info() {
			cout << " relation info\n";
			cout << "\t name: " << name() << endl;
			cout << "\t path: " << path() << endl;
		}

		void get_cnf(char *input, Schema *left, CNF &cnf_pred,
				Record &literal) {
			init_lexical_parser(input);
			if (yyparse() != 0) {
				cout << " Error: can't parse your CNF " << input << endl;
				exit(1);
			}
			cnf_pred.GrowFromParseTree(boolean, left, literal); // constructs CNF predicate
			close_lexical_parser();
		}

		void get_cnf(char *input, Schema *left, Schema *right, CNF &cnf_pred,
				Record &literal) {
			init_lexical_parser(input);
			if (yyparse() != 0) {
				cout << " Error: can't parse your CNF " << input << endl;
				exit(1);
			}
			cnf_pred.GrowFromParseTree(boolean, left, right, literal); // constructs CNF predicate
			close_lexical_parser();
		}

		void get_cnf(char *input, Schema *left, Function &fn_pred) {
			init_lexical_parser(input);
			if (yyparse() != 0) {
				cout << " Error: can't parse your arithmetic expr. " << input
						<< endl;
				exit(1);
			}
			fn_pred.GrowFromParseTree(finalFunction, *left); // constructs CNF predicate
			close_lexical_parser();
		}

		void get_cnf(char *input, CNF &cnf_pred, Record &literal) {
			init_lexical_parser(input);
			if (yyparse() != 0) {
				cout << " Error: can't parse your CNF.\n";
				exit(1);
			}
			cnf_pred.GrowFromParseTree(boolean, schema(), literal); // constructs CNF predicate
			close_lexical_parser();
		}

		void get_cnf(char *input, Function &fn_pred) {
			init_lexical_parser(input);
			if (yyparse() != 0) {
				cout << " Error: can't parse your CNF.\n";
				exit(1);
			}
			fn_pred.GrowFromParseTree(finalFunction, *(schema())); // constructs CNF predicate
			close_lexical_parser();
		}

		void get_cnf(CNF &cnf_pred, Record &literal) {
			cout << "\n enter CNF predicate (when done press ctrl-D):\n\t";
			if (yyparse() != 0) {
				cout << " Error: can't parse your CNF.\n";
				exit(1);
			}
			cnf_pred.GrowFromParseTree(boolean, schema(), literal); // constructs CNF predicate
		}

		void get_file_cnf(const char *fpath, CNF &cnf_pred, Record &literal) {
			yyin = fopen(fpath, "r");
			if (yyin == NULL) {
				cout << " Error: can't open file " << fpath
						<< " for parsing \n";
				exit(1);
			}
			if (yyparse() != 0) {
				cout << " Error: can't parse your CNF.\n";
				exit(1);
			}
			cnf_pred.GrowFromParseTree(boolean, schema(), literal); // constructs CNF predicate
			// cnf_pred.GrowFromParseTree (final, l_schema (), r_schema (), literal); // constructs CNF predicate over two relations l_schema is the left reln's schema r the right's
			//cnf_pred.Print ();
		}

		void get_sort_order(OrderMaker &sortorder) {
			cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
			if (yyparse() != 0) {
				cout << " Error: can't parse your CNF \n";
				exit(1);
			}
			Record literal;
			CNF sort_pred;
			sort_pred.GrowFromParseTree(boolean, schema(), literal); // constructs CNF predicate
			OrderMaker dummy;
			sort_pred.GetSortOrders(sortorder, dummy);
		}

		~relation() {
			delete rschema;
		}
	};

protected:
	// Objects declared here can be used by all tests in the test case for DBFile.
	char *dbfile_dir, *tpch_dir, *catalog_path; // full path of the catalog file
	relation *rel;

	BaseTest() {
		// You can do set-up work for each test here.
		// You can do set-up work for each test here.
		dbfile_dir = getenv("dbfile");// dir where binary heap files should be stored
		tpch_dir = getenv("tpch");// dir where dbgen tpch files (extension *.tbl) can be found
		catalog_path = "catalog";		// full path of the catalog file
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
				lineitem, supplier };
		cout
				<< " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
		cout << " catalog location: \t" << catalog_path << endl;
		cout << " tpch files dir: \t" << tpch_dir << endl;
		cout << " heap files dir: \t" << dbfile_dir << endl;
		cout << " \n\n";

		int findx = 0;
		while (findx < 1 || findx > 8) {
			cout << "\n select table: \n";
			cout << "\t 1. nation \n";
			cout << "\t 2. region \n";
			cout << "\t 3. customer \n";
			cout << "\t 4. part \n";
			cout << "\t 5. partsupp \n";
			cout << "\t 6. orders \n";
			cout << "\t 7. lineitem \n \t ";
			cout << "\t 8. supplier \n \t ";
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
