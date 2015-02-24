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

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen, bool asc) {
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
	fileName = new char[100];
	sprintf(fileName, "Phase1%s", ts.c_str());
	tmpFile.Open(0, fileName);
	tmpFile.Close();
	noOfRuns = 0;
	tmpRec = new Record();
	tmpPage = new Page();
	ascending = asc;
	pthread_t workerThread;
	pthread_create(&workerThread, NULL, &TPMMSHelper, (void *) this);
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
int BigQ::appendRunToFile(vector<Record*> aRun) {
	tmpFile.Open(1, fileName);
	int pageCountPerRun = 0;
	//aRun has already been sorted, put the records into file
	int size = aRun.size();
	// iterate over all Records and add to Pages and then to File.
	// list is in descending order and we add to file in ascending order.
	for (int i = 0; i < size; i++) {
		tmpRec->Copy(aRun.back());
		if (!tmpPage->Append(tmpRec)) {
			pageCountPerRun++;
			// if page is full, get the position to which we can append the page.
			int pos = !tmpFile.GetLength() ? 0 : tmpFile.GetLength() - 1;
			tmpFile.AddPage(tmpPage, pos);
			tmpPage->EmptyItOut();
			tmpPage->Append(tmpRec);
		}
		// remove the record added to the page from Vector.
		aRun.pop_back();
	}

	// add the last page to the File.
	int pos = !tmpFile.GetLength() ? 0 : tmpFile.GetLength() - 1;
	tmpFile.AddPage(tmpPage, pos);
	tmpPage->EmptyItOut();

	// Keep track of the number of pages in this run.
	noOfPages.push_back(pageCountPerRun + 1);
	tmpFile.Close();
	return pageCountPerRun + 1;
}

// Returns the record in minRec and bucket number in pos;
int BigQ::GetTop(Record* minRec) {
	if (recordBuffer.size() > 0) {
		int pos = 0;
		minRec->Copy(recordBuffer.at(0));
		if (ascending) {
			// return min record.
			for (int i = 1; i < recordBuffer.size(); i++) {
				if (lessThan(recordBuffer.at(i), minRec, sortOrder)) {
					minRec->Copy(recordBuffer.at(i));
					pos = i;
				}
			}
		} else {
			// return max record
			for (int i = 1; i < recordBuffer.size(); i++) {
				if (!lessThan(recordBuffer.at(i), minRec, sortOrder)) {
					minRec->Copy(recordBuffer.at(i));
					pos = i;
				}
			}
		}
		return pos;
	}
	return -1;
}

// Update Record Buffer and other buffers during Merge Phase.
int BigQ::updateRecordBuffer(int i) {
	// if there is a record in the page buffer, then get it and update record buffer.
	if (pageBuffer.at(i)->GetFirst(tmpRec)) {
		recordBuffer.at(i)->Consume(tmpRec);// update in vector works by direct access, rather than using insert.
		return 1;
	} else {
		// pagebuffer is empty.
		if (PageCtrPerRun.at(i) < noOfPages.at(i)) {
			// if there are more pages available in the run, get it.
			int pos = startPageIndex[i] + PageCtrPerRun[i];
			tmpFile.GetPage(pageBuffer.at(i), pos); // update page buffer.
			PageCtrPerRun.at(i) = PageCtrPerRun.at(i) + 1; // increment page count for the run.
			pageBuffer.at(i)->GetFirst(tmpRec);	//update record buffer.
			recordBuffer.at(i)->Consume(tmpRec);
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

	pageBuffer.reserve(noOfRuns);
	recordBuffer.reserve(noOfRuns);
	startPageIndex.reserve(noOfRuns);
	PageCtrPerRun.reserve(noOfRuns);
	cout << "No of Runs" << noOfRuns << endl;

// Initialize Page Ctr and Start Index Array.
	PageCtrPerRun.insert(PageCtrPerRun.begin(), noOfRuns, 0);
	startPageIndex.insert(startPageIndex.begin(), 0);

	for (int i = 1; i < noOfRuns; i++) {
		int val = startPageIndex.at(i - 1) + noOfPages.at(i - 1);
		startPageIndex.insert(startPageIndex.begin() + i, val);
	}

	// Fetch 1st page from each run and put them in PageBuffer. Add 1st Record from each. Assuming that there is atleast one page and one record in each run.
	for (int i = 0; i < noOfRuns; i++) {
		int pos = startPageIndex.at(i) + PageCtrPerRun.at(i);
		Page *tPage = new Page();
		tmpFile.GetPage(tPage, pos);
		PageCtrPerRun.at(i) = PageCtrPerRun.at(i) + 1;// increment page count for the run.
		pageBuffer.insert(pageBuffer.begin() + i, tPage);// update page buffer.
		Record *tRec = new Record();
		pageBuffer.at(i)->GetFirst(tRec);	//update record buffer.
		recordBuffer.insert(recordBuffer.begin() + i, tRec);
	}
	int recCtr = 0;
	// while there are records in the record buffer.
	while (recordBuffer.size() > 0) {
		int i;
		i = GetTop(tmpRec);
		if (i == -1) {
			break;
		}
		Record *tRec = new Record();
		tRec->Consume(tmpRec);
		// push element through out-pipe
		output->Insert(tRec);
		updateRecordBuffer(i);
		recCtr++;
	}

	pageBuffer.clear();
	PageCtrPerRun.clear();
	startPageIndex.clear();
	recordBuffer.clear();

	output->ShutDown();
	return recCtr;
}

int BigQ::generateRuns() {
	int recCtr = 0;
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
				if (ascending) {
					sort(aRunVector.begin(), aRunVector.end(),
							CompareRecordsAscending(sortOrder));
				} else {
					sort(aRunVector.begin(), aRunVector.end(),
							CompareRecordsDescending(sortOrder));
				}
				appendRunToFile(aRunVector);
				//now clear the vector to begin for new run
				aRunVector.clear();
				noOfRuns++;
			}
			currentPage.Append(&recFromPipe); // add the record which could not be added to the page.
		}
		aRunVector.push_back(copyRec); // push back the copy onto vector
		recCtr++;
	} // Input pipe is empty.
	  // if there is anything in vector it should be sorted and written out to file
	if (aRunVector.size() > 0) {
		pageCountPerRun = 0; //reset pageCountPerRun for next run as current run is full
		if (ascending) {
			sort(aRunVector.begin(), aRunVector.end(),
					CompareRecordsAscending(sortOrder));
		} else {
			sort(aRunVector.begin(), aRunVector.end(),
					CompareRecordsDescending(sortOrder));
		}
		appendRunToFile(aRunVector);
		//now clear the vector to begin for new run
		aRunVector.clear();
		noOfRuns++;
	}
	return 1;
}

void* BigQ::TPMMS() {

	generateRuns();
	MergeRuns();
	remove(fileName);
}

void* BigQ::TPMMSHelper(void* context) {
	((BigQ *) context)->TPMMS();
}
