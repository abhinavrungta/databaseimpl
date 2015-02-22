#include <pthread/pthread.h>
#include <sys/_pthread/_pthread_t.h>
#include <cstdio>
#include <iostream>

#include "../gtest/gtest.h"
#include "BigQ.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Pipe.h"
#include "Record.h"
#include "TestBase.h"

// The fixture for testing class DBFile.
class BigQTest: public BaseTest {
public:
	static void* ProducerHelper(void* arg) {
		((testutil *) arg)->obj->producer(arg);
	}

	static void* ConsumerHelper(void* arg) {
		((testutil *) arg)->obj->consumer(arg);
	}
protected:
	int runlen;
	typedef struct {
		Pipe *pipe;
		OrderMaker *order;
		bool print;
		bool write;
		BigQTest *obj;
	} testutil;

	BigQTest() {
	}

	virtual ~BigQTest() {
		// You can do clean-up work that doesn't throw exceptions here.
	}

	void* producer(void *arg) {
		testutil *t = (testutil *) arg;
		Pipe *myPipe = (Pipe *) t->pipe;

		Record temp;
		int counter = 0;

		DBFile dbfile;
		dbfile.Open(rel->path());
		cout << " producer: opened DBFile " << rel->path() << endl;
		dbfile.MoveFirst();

		while (dbfile.GetNext(temp) == 1) {
			counter += 1;
			if (counter % 100000 == 0) {
				cerr << " producer: " << counter << endl;
			}
			myPipe->Insert(&temp);
		}
		dbfile.Close();
		myPipe->ShutDown();

		cout << " producer: inserted " << counter << " recs into the pipe\n";
	}

	void* consumer(void *arg) {
		testutil *t = (testutil *) arg;
		ComparisonEngine ceng;

		DBFile dbfile;
		char outfile[100];

		if (t->write) {
			sprintf(outfile, "%s.bigq", rel->path());
			dbfile.Create(outfile, heap, NULL);
		}

		int err = 0;
		int i = 0;

		Record rec[2];
		Record *last = NULL, *prev = NULL;

		while (t->pipe->Remove(&rec[i % 2])) {
			prev = last;
			last = &rec[i % 2];
			if (prev && last) {
				if (ceng.Compare(prev, last, t->order) == 1) {
					err++;
				}
				if (t->write) {
					dbfile.Add(*prev);
				}
			}
			if (t->print) {
				last->Print(rel->schema());
			}
			i++;
		}

		cout << " consumer: removed " << i << " recs from the pipe\n";
		if (t->write) {
			if (last) {
				dbfile.Add(*last);
			}
			cerr << " consumer: recs removed written out as heap DBFile at "
					<< outfile << endl;
			dbfile.Close();
		}
		cerr << " consumer: " << (i - err) << " recs out of " << i
				<< " recs in sorted order \n";
		if (err) {
			cerr << " consumer: " << err << " recs failed sorted order test \n"
					<< endl;
		}
	}
};

/* Options
 1. sort
 2. sort + display
 3. sort + write
 */
TEST_F(BigQTest, Sort) {
	int option = 0;
	while (option < 1 || option > 3) {
		cout << " select test option: \n";
		cout << " \t 1. sort \n";
		cout << " \t 2. sort + display \n";
		cout << " \t 3. sort + write \n\t ";
		cin >> option;
	}

	cout << "\t\n specify runlength:\n\t ";
	cin >> runlen;
	// sort order for records
	OrderMaker sortorder;
	rel->get_sort_order(sortorder);

	int buffsz = 100; // pipe cache size
	Pipe input(buffsz);
	Pipe output(buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	testutil tutil1 = { &input, &sortorder, false, false, this };
	pthread_create(&thread1, NULL, &ProducerHelper, (void *) &tutil1);

	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	testutil tutil2 = { &output, &sortorder, false, false, this };
	if (option == 2) {
		tutil2.print = true;
	} else if (option == 3) {
		tutil2.write = true;
	}
	pthread_create(&thread2, NULL, &ConsumerHelper, (void *) &tutil2);

	BigQ bq(input, output, sortorder, runlen);

	pthread_join(thread1, NULL);
	pthread_join(thread2, NULL);
}
