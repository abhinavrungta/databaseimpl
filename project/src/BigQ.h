#include <vector>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Pipe.h"
#include "Record.h"

using namespace std;

class BigQ {
	Pipe *input;
	Pipe *output;
	OrderMaker *sortOrder;
	int runSize;
	char *fileName;
	File tmpFile;
	int noOfRuns;
	vector<int> noOfPages;
	vector<int> startPageIndex;
	vector<int> PageCtrPerRun;
	vector<Record*> recordBuffer;
	vector<Page*> pageBuffer;

	Record *tmpRec;
	Page *tmpPage;

	bool ascending;

	struct CompareRecordsAscending {
		OrderMaker *pSortOrder;
		CompareRecordsAscending(OrderMaker *order) :
				pSortOrder(order) {
		}

		bool operator()(Record* const & r1, Record* const & r2) {
			Record* r11 = const_cast<Record*>(r1);
			Record* r22 = const_cast<Record*>(r2);

			ComparisonEngine ce;
			//sort in a descending order, 'cause we fetch it reversely
			if (ce.Compare(r11, r22, pSortOrder) > 0) {
				return true;
			} else
				return false;
		}
	};

	struct CompareRecordsDescending {
		OrderMaker *pSortOrder;
		CompareRecordsDescending(OrderMaker *order) :
				pSortOrder(order) {
		}

		bool operator()(Record* const & r1, Record* const & r2) {
			Record* r11 = const_cast<Record*>(r1);
			Record* r22 = const_cast<Record*>(r2);

			ComparisonEngine ce;
			//sort in a ascending order, 'cause we fetch it reversely
			if (ce.Compare(r11, r22, pSortOrder) < 0) {
				return true;
			} else
				return false;
		}
	};
private:
	bool lessThan(Record *r1, Record *r2, OrderMaker *sortorder);
	int appendRunToFile(vector<Record*> aRun);
	int GetTop(Record* minRec);
	int updateRecordBuffer(int i);

public:
	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen,
			bool asc = true);
	~BigQ();
	int generateRuns();
	int MergeRuns();
	void* TPMMS();

private:
	static void* TPMMSHelper(void*);
};
