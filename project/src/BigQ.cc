#include "BigQ.h"

#include <pthread/pthread.h>
#include <sys/_pthread/_pthread_t.h>
#include <sys/_types/_time_t.h>
#include <algorithm>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	input = &in;
	output = &out;
	sortOrder = &sortorder;
	runSize = runlen;

	// create a timestamp based tempFile for Phase1.
	time_t seconds;
	time(&seconds);
	std::stringstream ss;
	ss << seconds;
	string ts = ss.str();
	cout << ts << endl;
	fileName = new char[100];
	sprintf(fileName, "Phase1%s", ts.c_str());
	tmpFile.Open(0, fileName);
	tmpFile.Close();

	noOfRuns = 0;
	pthread_t workerThread;
	pthread_create(&workerThread, NULL, TPMMS, (void *) this);
}

BigQ::~BigQ() {
	tmpFile.Close();
}

// returns true if Record r1 < Record r2
bool BigQ::lessThan(Record *r1, Record *r2, OrderMaker *sortorder) {
	ComparisonEngine cmp;

	if (cmp.Compare(r1, r2, sortorder) <= 0)
		return true;
	else
		return false;
}

// Append a sorted Run to File in Phase1.
void BigQ::appendRunToFile(vector<Record*> aRun) {
	tmpFile.Open(1, fileName);
	int pageCountPerRun = 0;
	//aRun has already been sorted, put the records into file
	Page tmpPage;
	Record copyRec;
	int size = aRun.size();
	// iterate over all Records and add to Pages and then to File.
	// list is in descending order and we add to file in ascending order.
	for (int i = 0; i < size; i++) {
		copyRec.Copy(aRun.back());
		if (!tmpPage.Append(&copyRec)) {
			pageCountPerRun++;
			// if page is full, get the position to which we can append the page.
			int pos = !tmpFile.GetLength() ? 0 : tmpFile.GetLength() - 1;
			tmpFile.AddPage(&tmpPage, pos);
			tmpPage.EmptyItOut();
			tmpPage.Append(&copyRec);
		}
		// remove the record added to the page from Vector.
		aRun.pop_back();
	}

	// add the last page to the File.
	int pos = !tmpFile.GetLength() ? 0 : tmpFile.GetLength() - 1;
	tmpFile.AddPage(&tmpPage, pos);
	tmpPage.EmptyItOut();
	delete tmpPage;

	// Keep track of the number of pages in this run.
	noOfPages.push_back(pageCountPerRun + 1);
	tmpFile.Close();
}

// Returns the record in minRec and bucket number in pos;
int BigQ::GetMin(Record* minRec) {
	if (recordBuffer.size() > 0) {
		int pos = 0;
		minRec->Copy(&recordBuffer[0]);
		for (int i = 1; i < recordBuffer.size(); i++) {
			if (lessThan(&recordBuffer[i], minRec, sortOrder)) {
				minRec->Copy(&recordBuffer[i]);
				pos = i;
			}
		}
		return pos;
	}
	return -1;
}

// Update Record Buffer and other buffers during Merge Phase.
int BigQ::updateRecordBuffer(int i) {
	Record tmpRec;
	Page tmpPage;
	// if there is a record in the page buffer, then get it and update record buffer.
	if (!pageBuffer[i].GetFirst(&tmpRec)) {
		recordBuffer[i] = tmpRec;
		return 1;
	} else {
		// pagebuffer is empty.
		if (PageCtrPerRun[i] < noOfPages[i]) {
			// if there are more pages available in the run, get it.
			int pos = startPageIndex[i] + PageCtrPerRun[i];
			tmpFile.GetPage(&tmpPage, pos);
			PageCtrPerRun[i]++;		// increment page count for the run.
			pageBuffer[i] = tmpPage;		// update page buffer.
			pageBuffer[i].GetFirst(&tmpRec);	//update record buffer.
			recordBuffer[i] = tmpRec;
			return 1;
		} else {
			// no more pages are available, run completed. Update books.
			noOfPages.erase(noOfPages.begin() + i);
			pageBuffer.erase(pageBuffer.begin() + i);
			recordBuffer.erase(recordBuffer.begin() + i);
			startPageIndex.erase(startPageIndex.begin() + i);
			PageCtrPerRun.erase(PageCtrPerRun.begin() + i);
			return 0;
		}
	}
}

// Merge Runs by reading runs from Phase1 file.
int BigQ::MergeRuns() {
	tmpFile.Close();
	tmpFile.Open(1, fileName);
	if (noOfRuns < 1) {
		return 0;
	}

	startPageIndex = new int[noOfRuns]; //index of the header at each run
	PageCtrPerRun = new int[noOfRuns]; //index of the header at each run
	recordBuffer = new Record[noOfRuns]; // m-1 header record of m-1 header pages
	pageBuffer = new Page[noOfRuns]; // m-1 header pages of m-1 runs

	if (!startPageIndex || !PageCtrPerRun || !recordBuffer || !pageBuffer) {
		cerr << "allocation failed!" << endl;
	}

// Initialize Page Ctr and Start Index Array.
	startPageIndex[0] = 0;
	PageCtrPerRun[0] = 0;
	for (int i = 1; i < noOfRuns; i++) {
		startPageIndex[i] = startPageIndex[i - 1] + noOfPages.at(i - 1);
		PageCtrPerRun[i] = 0;
	}

// Fetch 1st page from each run and put them in PageBuffer. Add 1st Record from each. Assuming that there is atleast one page and one record in each run.
	for (int i = 0; i < noOfRuns; i++) {
		updateRecordBuffer(i);
	}

	// while there are records in the record buffer.
	while (recordBuffer.size() > 0) {
		Record tmpRec;
		int i = GetMin(&tmpRec);
		// push min element through out-pipe
		output->Insert(&tmpRec);
		if (i == -1) {
			break;
		}
		updateRecordBuffer(i);
	}

	delete[] pageBuffer;
	delete[] PageCtrPerRun;
	delete[] startPageIndex;
	delete[] recordBuffer;
	return 1;
}

void* BigQ::TPMMS(void *arg) {
	Record recFromPipe;
	vector<Record*> aRunVector;
	Page currentPage;
	int pageCountPerRun = 0;

// while there are records in the input pipe.
	while (input->Remove(&recFromPipe)) {
		Record *copyRec = new Record();
		copyRec->Copy(&recFromPipe); //Copy Record as currentPage.Append() would consume the record

		if (!currentPage.Append(&recFromPipe)) {
			//page full, start new page and increase the page count.
			pageCountPerRun++;
			currentPage.EmptyItOut();
			if (pageCountPerRun >= runSize) {
				//If pageCount >= runSize; Sort the list of records and write the run to file.
				pageCountPerRun = 0; //reset pageCountPerRun for next run as current run is full
				sort(aRunVector.begin(), aRunVector.end(),
						CompareRecords(sortOrder));
				appendRunToFile(aRunVector);
				//now clear the vector to begin for new run
				aRunVector.clear();
				noOfRuns++;
			}
			currentPage.Append(&recFromPipe); // add the record which could not be added to the page.
		}
		aRunVector.push_back(copyRec); // push back the copy onto vector
	} // Input pipe is empty.

// if there is anything in vector it should be sorted and written out to file
	if (aRunVector.size() > 0) {
		pageCountPerRun = 0; //reset pageCountPerRun for next run as current run is full
		sort(aRunVector.begin(), aRunVector.end(), CompareRecords(sortOrder));
		appendRunToFile(aRunVector);
		//now clear the vector to begin for new run
		aRunVector.clear();
		noOfRuns++;
	}

	// Merge
	MergeRuns();
	output->ShutDown();
}
