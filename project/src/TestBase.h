#include <iostream>
#include "Record.h"
#include "gtest/gtest.h"

// The fixture for testing class DBFile.
class BaseTest: public testing::Test {
protected:

	BaseTest() {
		// You can do set-up work for each test here.
	}

	virtual ~BaseTest() {
		// You can do clean-up work that doesn't throw exceptions here.
	}

	// If the constructor and destructor are not enough for setting up
	// and cleaning up each test, you can define the following methods:
	virtual void SetUp() {
	}

	virtual void TearDown() {
	}

	bool RecordCompare(Record *a, Record *b) {
		if (strcmp(a->GetBits(), b->GetBits()) == 0) {
			return true;
		}
		return false;
	}
};
