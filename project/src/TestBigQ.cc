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

typedef struct {
	Pipe *inputPipe;
	Pipe *outputPipe;
	OrderMaker *order;
	BaseTest::relation *rel;
	bool print;
	bool write;
} testutil;

class TestFactory {
public:
	testutil *t;
	virtual int producer() {
	}
	virtual int consumer() {
	}
	TestFactory(void *params) {
		t = (testutil *) params;
	}
	virtual ~TestFactory() {
	}
};

class Test1: public TestFactory {
public:
	int counter = 0;
	Test1(void *params) :
			TestFactory(params) {
	}
	virtual ~Test1() {
	}
	virtual int producer() {
		Pipe *myPipe = (Pipe *) t->inputPipe;

		Record temp;

		DBFile dbfile;
		dbfile.Open(t->rel->path());
		cout << " producer: opened DBFile " << t->rel->path() << endl;
		dbfile.MoveFirst();

		while (dbfile.GetNext(temp) == 1) {
			counter += 1;
			myPipe->Insert(&temp);
		}
		dbfile.Close();
		myPipe->ShutDown();

		cout << " producer: inserted " << counter << " recs into the pipe\n";
		return 1;
	}

	virtual int consumer() {
		ComparisonEngine ceng;

		DBFile dbfile;
		char outfile[100];

		if (t->write) {
			sprintf(outfile, "%s.bigq", t->rel->path());
			dbfile.Create(outfile, heap, NULL);
			dbfile.Open(outfile);
		}

		int err = 0;
		int i = 0;

		Record rec[2];
		Record *last = NULL, *prev = NULL;

		while (t->outputPipe->Remove(&rec[i % 2])) {
			prev = last;
			last = &rec[i % 2];
			if (prev && last) {
				if (ceng.Compare(prev, last, t->order) > 0) {
					err++;
				}
				if (t->write) {
					dbfile.Add(*prev);
				}
			}
			if (t->print) {
				last->Print(t->rel->schema());
			}
			i++;
		}

		cout << " consumer: removed " << i << " recs from the pipe\n";
		if (t->write) {
			if (last) {
				dbfile.Add(*last);
			}
			cout << " consumer: recs removed written out as heap DBFile at "
					<< outfile << endl;
			dbfile.Close();
		}
		EXPECT_EQ(counter, i);
		EXPECT_EQ(0, err);
		return 1;
	}
};

class Test2: public TestFactory {
public:
	int counter = 0;
	Test2(void *params) :
			TestFactory(params) {
	}
	virtual ~Test2() {
	}
	virtual int producer() {
		Pipe *myPipe = (Pipe *) t->inputPipe;
		Record temp;
		DBFile dbfile;
		dbfile.Open(t->rel->path());
		cout << " producer: opened DBFile " << t->rel->path() << endl;
		dbfile.MoveFirst();
		while (dbfile.GetNext(temp) == 1) {
			counter += 1;
			myPipe->Insert(&temp);
		}
		dbfile.Close();
		myPipe->ShutDown();
		cout << " producer: inserted " << counter << " recs into the pipe\n";
		return 1;
	}

	virtual int consumer() {
		ComparisonEngine ceng;

		DBFile dbfile;
		char outfile[100];

		if (t->write) {
			sprintf(outfile, "%s.reverse.bigq", t->rel->path());
			dbfile.Create(outfile, heap, NULL);
			dbfile.Open(outfile);
		}

		int err = 0;
		int i = 0;

		Record rec[2];
		Record *last = NULL, *prev = NULL;

		while (t->outputPipe->Remove(&rec[i % 2])) {
			prev = last;
			last = &rec[i % 2];
			if (prev && last) {
				if (ceng.Compare(prev, last, t->order) < 0) {
					err++;
				}
				if (t->write) {
					dbfile.Add(*prev);
				}
			}
			if (t->print) {
				last->Print(t->rel->schema());
			}
			i++;
		}

		cout << " consumer: removed " << i << " recs from the pipe\n";
		if (t->write) {
			if (last) {
				dbfile.Add(*last);
			}
			cout << " consumer: recs removed written out as heap DBFile at "
					<< outfile << endl;
			dbfile.Close();
		}
		EXPECT_EQ(counter, i);
		EXPECT_EQ(0, err);
		return 1;
	}
};

// The fixture for testing class DBFile.
class BigQTest: public BaseTest {
public:
	static void* ProducerHelper(void* arg) {
		((TestFactory *) arg)->producer();
	}

	static void* ConsumerHelper(void* arg) {
		((TestFactory *) arg)->consumer();
	}

protected:
	int runlen;
	BigQTest() {
	}

	virtual ~BigQTest() {
		// You can do clean-up work that doesn't throw exceptions here.
	}
};

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
	testutil tutil = { &input, &output, &sortorder, rel, false, false };
	if (option == 2) {
		tutil.print = true;
	} else if (option == 3) {
		tutil.write = true;
	}
	TestFactory *obj = new Test1((void *) &tutil);
	pthread_create(&thread1, NULL, &ProducerHelper, (void *) obj);
	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	pthread_create(&thread2, NULL, &ConsumerHelper, (void *) obj);
	BigQ bq(input, output, sortorder, runlen);

	pthread_join(thread1, NULL);
	pthread_join(thread2, NULL);
}

TEST_F(BigQTest, ReverseSort) {
	int option = 3;
	int runlen = 10;
	// sort order for records
	OrderMaker sortorder;
	rel->get_sort_order(sortorder);

	int buffsz = 100; // pipe cache size
	Pipe input(buffsz);
	Pipe output(buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	testutil tutil = { &input, &output, &sortorder, rel, false, false };
	if (option == 2) {
		tutil.print = true;
	} else if (option == 3) {
		tutil.write = true;
	}
	TestFactory *obj = new Test2((void *) &tutil);
	pthread_create(&thread1, NULL, &ProducerHelper, (void *) obj);
	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	pthread_create(&thread2, NULL, &ConsumerHelper, (void *) obj);
	BigQ bq(input, output, sortorder, runlen, false);

	pthread_join(thread1, NULL);
	pthread_join(thread2, NULL);
}

TEST_F(BigQTest, SortReverseInput) {
	int option = 3;
	int runlen = 20;
	// sort order for records
	OrderMaker sortorder;
	rel->get_sort_order(sortorder);

	int buffsz = 100; // pipe cache size
	Pipe input(buffsz);
	Pipe output(buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	testutil tutil = { &input, &output, &sortorder, rel, false, true };

	TestFactory *obj = new Test2((void *) &tutil);
	pthread_create(&thread1, NULL, &ProducerHelper, (void *) obj);
	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	pthread_create(&thread2, NULL, &ConsumerHelper, (void *) obj);
	BigQ bq(input, output, sortorder, runlen, false);

	pthread_join(thread1, NULL);
	pthread_join(thread2, NULL);

	// sort reversed file.
	option = 1;
	// update file name to read the rel from
	char path[100];
	sprintf(path, "%s.reverse.bigq", rel->path());
	rel->setPath(path);

	Pipe input1(buffsz);
	Pipe output1(buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread11;
	testutil tutil1 = { &input1, &output1, &sortorder, rel, false, false };

	TestFactory *obj1 = new Test1((void *) &tutil1);
	pthread_create(&thread11, NULL, &ProducerHelper, (void *) obj1);
	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread22;
	pthread_create(&thread22, NULL, &ConsumerHelper, (void *) obj1);
	BigQ bq1(input1, output1, sortorder, runlen);

	pthread_join(thread11, NULL);
	pthread_join(thread22, NULL);
}

TEST_F(BigQTest, SmallRunLength) {
	int option = 1;
	runlen = 1;
	// sort order for records
	OrderMaker sortorder;
	rel->get_sort_order(sortorder);

	int buffsz = 100; // pipe cache size
	Pipe input(buffsz);
	Pipe output(buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	testutil tutil = { &input, &output, &sortorder, rel, false, false };
	if (option == 2) {
		tutil.print = true;
	} else if (option == 3) {
		tutil.write = true;
	}
	TestFactory *obj = new Test1((void *) &tutil);
	pthread_create(&thread1, NULL, &ProducerHelper, (void *) obj);
	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	pthread_create(&thread2, NULL, &ConsumerHelper, (void *) obj);
	BigQ bq(input, output, sortorder, runlen);

	pthread_join(thread1, NULL);
	pthread_join(thread2, NULL);
}
