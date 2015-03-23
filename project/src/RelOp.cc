#include "RelOp.h"
#include <pthread.h>
#include <iostream>
#include <string>
#include <sstream>
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Record.h"
#include "Schema.h"


// -------------------------SELECT FILE -------------------------------------------

void *SelectFile::selectFile(void *arg) {
	SelectFile *sf = (SelectFile *) arg;

	//cout << "created thread \n";
	sf->DoSelectFile();
	return NULL;
}

void SelectFile::DoSelectFile() {
	Record *tmp = new Record;
	
	while (this->inFile->GetNext(*tmp, *(this->selOp), *(this->literal))) {
		this->outPipe->Insert(tmp);
	}

	delete tmp;
	this->inFile->Close();
	this->outPipe->ShutDown();
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp,	Record &literal) {
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
 	this->literal = &literal;
	pthread_create(&thread, NULL, selectFile, this);
} 

void SelectFile::WaitUntilDone() {
	// pthread_join (thread, NULL);
	pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int n) {
	this->nPages = n;
}


// ------------------- SELECT PIPE ----------------------------------

void *SelectPipe::selectPipe(void *arg) {
	SelectPipe *sp = (SelectPipe *) arg;
	sp->DoSelectPipe();
	return NULL;
}

void SelectPipe::DoSelectPipe() {
	Record *tmp = new Record;
	ComparisonEngine comp;
	// get Record from input pipe, compare with given CNF and push to output pipe.
	while (this->inPipe->Remove(tmp)) {
		if (comp.Compare(tmp, this->literal, this->selOp)) {
			this->outPipe->Insert(tmp);
		}
		delete tmp;
		this->outPipe->ShutDown();
	}
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {

	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal->Copy(&literal);
	pthread_create(&thread, NULL, selectPipe, this);
}

void SelectPipe::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void SelectPipe::Use_n_Pages(int n) {
	this->nPages = n;
}


//------------------------------PROJECT-----------------------------------------------------


void *Project::project(void *arg) {
	Project *pr = (Project *) arg;
	pr->DoProject();
	return NULL;

}

void Project::DoProject() {
	Record *tmp = new Record;

	while (this->inPipe->Remove(tmp)) {
		tmp->Project(this->keepMe, this->numAttsOutput, this->numAttsInput);
		this->outPipe->Insert(tmp);
	}
	delete tmp;
	this->outPipe->ShutDown();
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {

	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	pthread_create(&thread, NULL, project, this);
}

void Project::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void Project::Use_n_Pages(int n) {
	this->nPages = n;
}


// ----------------JOIN---------------------------------------------------------------

void *Join::join(void* arg) {
	Join *jn = (Join *) arg;
	jn->DoJoin();
	return NULL;
} 

void Join::DoJoin() {


        Pipe *LO = new Pipe(100);
        Pipe *RO = new Pipe(100);

        OrderMaker *omL = new OrderMaker();
        OrderMaker *omR = new OrderMaker();
    
	ComparisonEngine compEng;

        int sortMergeFlag = selOp->GetSortOrders(*omL, *omR);

        // if sort merge flag != 0 perform SORT-MERGE JOIN
        if (sortMergeFlag != 0) {

                // sort left pipe
                BigQ L(*inPipeL, *LO, *omL, this->nPages);

                // sort right pipe
                BigQ R(*inPipeR, *RO, *omR, this->nPages);

                Record RL;
                Record *RR = new Record();
                vector<Record*> mergeVector; 

                int isLeftPipeEmpty = LO->Remove(&RL);
                int isRightPipeEmpty = RO->Remove(RR);

                int numLeftAttrs = RL.GetNumAtts();
                int numRightAttrs = RR->GetNumAtts();

                int attrsToKeep[numLeftAttrs + numRightAttrs];
             
	        int k = 0;
             
		for (int i = 0; i < numLeftAttrs; i++) {
                        attrsToKeep[k++] = i;
                }

                for (int i = 0; i < numRightAttrs; i++) {
                        attrsToKeep[k++] = i;
                }

                Record mergedRecord;
                int mergedRecCount = 0;

                while (isLeftPipeEmpty != 0 && isRightPipeEmpty != 0) {
                        
                        int orderMakerAnswer = compEng.Compare(&RL, omL, RR, omR);
                        
                        if (orderMakerAnswer == 0) {
     
                                for (int i = 0; i < mergeVector.size(); i++) {
                                        delete mergeVector[i];
                                        mergeVector[i] = NULL;
                                }
                                mergeVector.clear();
                                
                                while (orderMakerAnswer == 0 && isRightPipeEmpty != 0) {
                                        mergeVector.push_back(RR);
                                        RR = new Record();
                                        isRightPipeEmpty = RO->Remove(RR);
                                        if (isRightPipeEmpty != 0) {
                                                orderMakerAnswer = compEng.Compare(&RL, omL, RR, omR);
                                        }
                                }

                                orderMakerAnswer = compEng.Compare(&RL, omL, mergeVector[0], omR);
                                
                                while (orderMakerAnswer == 0 && isLeftPipeEmpty != 0) {

                                        for (int i = 0; i < mergeVector.size(); i++) {
                                                mergedRecord.MergeRecords(&RL, mergeVector[i], numLeftAttrs, numRightAttrs, attrsToKeep, numLeftAttrs + numRightAttrs, numLeftAttrs);
                                                outPipe->Insert(&mergedRecord);
                                                mergedRecCount++;
                                        }
                                        
                                        isLeftPipeEmpty = LO->Remove(&RL);
                                        orderMakerAnswer = compEng.Compare(&RL, omL, mergeVector[0], omR);

                                }

                        } 
                        else if (orderMakerAnswer < 0) {
                                isLeftPipeEmpty = LO->Remove(&RL);                                
                        } 
                        else {
                                isRightPipeEmpty = RO->Remove(RR);
                        }
                }
                cout << "Total Records Merged :: " << mergedRecCount << endl;

        }
        else {

		int mergedRecCount = 0;
                
		Page tmpPage;
		DBFile rightRelationFile;
		rightRelationFile.Create((char*) "tmpL", heap, NULL);
		rightRelationFile.Open("tmpL");
 
//		DBFile rightRelationFIle;
		Record RR;
//		rightRelationFIle.Create(fileName, heap, NULL);

                while (inPipeR->Remove(&RR)) {
                        rightRelationFile.Add(RR);
                }
                rightRelationFile.Close();

                vector<Record*> leftRelationVector;
                int numPagesAllocatedForLeft = 0;
                Page pageForLeftRecords;
                Record leftRecord;
                Record* rec = new Record();

                int isLeftRecordPresent = inPipeL->Remove(&leftRecord);
                bool isPipeEnded = false;
                int isRecordAdded;
                
                while (isLeftRecordPresent || isPipeEnded) {
                        
                        // Start reading left Record into a page
                        if (isLeftRecordPresent) {
                                isRecordAdded = pageForLeftRecords.Append(&leftRecord);
                        }
                        // when page is full or pipe is empty
                        if (isRecordAdded == 0 || isPipeEnded) {

                                // increment number of pages used
                                numPagesAllocatedForLeft++;
                                
                                // flush records of the page into vector
                                while (pageForLeftRecords.GetFirst(rec)) {
                                        leftRelationVector.push_back(rec);
                                        rec = new Record();
                                }
                                
                                //For block nested loop join, n-1 pages of left relation are joined with each record of right relation
                                
                                // start reading right relation from file when n - 1 buffer pages are full OR pipe is empty
                                if (numPagesAllocatedForLeft == this->nPages - 1 || isPipeEnded) {

                                        rightRelationFile.Open("tmpL");
                                        rightRelationFile.MoveFirst();
                                        Record rightRec;
                                        int isRightRecPresent = rightRelationFile.GetNext(rightRec);
                                        
                                        Record mergedRecord;
                                        int numLeftAttrs = leftRelationVector[0]->GetNumAtts();
                                        int numRightAttrs = rightRec.GetNumAtts();
                                        int attrsToKeep[numLeftAttrs + numRightAttrs];
                                        int k = 0;
                                        for (int i = 0; i < numLeftAttrs; i++) {
                                                attrsToKeep[k++] = i;
                                        }
                                        for (int i = 0; i < numRightAttrs; i++) {
                                                attrsToKeep[k++] = i;
                                        }

                                        // while right relation file has next record
                                        while (isRightRecPresent != 0) {
                                                for (int i = 0; i < leftRelationVector.size(); i++) {
                                                        
                                                        int isAccepted = compEng.Compare(leftRelationVector[i], &rightRec, literal, selOp);
                                                       
                                                        // merge records when the cnf is accepted
                                                        if (isAccepted != 0) {
                                                                mergedRecord.MergeRecords(leftRelationVector[i], &rightRec, numLeftAttrs, numRightAttrs, attrsToKeep, numLeftAttrs + numRightAttrs, numLeftAttrs);
                                                                outPipe->Insert(&mergedRecord);
                                                                mergedRecCount++;
                                                        }
                                                }
                                                isRightRecPresent = rightRelationFile.GetNext(rightRec);
                                        }
                                        rightRelationFile.Close();

                                        // flush the vector 
                                        numPagesAllocatedForLeft = 0;
                                        for (int i = 0; i < leftRelationVector.size(); i++) {
                                                delete leftRelationVector[i];
                                                leftRelationVector[i] = NULL;
                                        }
                                        leftRelationVector.clear();
                                        
                                        // exit loop is pipe is empty
                                        if (isPipeEnded)
                                                break;
                                }
                        }
                        
                        isLeftRecordPresent = inPipeL->Remove(&leftRecord);
                        if (isLeftRecordPresent == 0) {
                                isPipeEnded = true;
                        }
                }
                cout << "Total Records Merged :: " << mergedRecCount << endl;
                remove("tmpL");
        }        
        
        outPipe->ShutDown();
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&thread, NULL, join, this);

}

void Join::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void Join::Use_n_Pages(int n) {
	this->nPages = n;
}

//--------SUM------------------------------

void *Sum::sum(void *arg) {
	Sum *sm = (Sum *) arg;
	sm->DoSum();
	return NULL;
}


void Sum::DoSum() {
        cout << "Starting Summation" << endl;

        //struct RunParams* params = (struct RunParams*) parameters;

        int intSum = 0;
        double doubleSum = 0.0;
        int intAttrVal = 0;
        double doubleAttrVal = 0.0;
        Record rec;
        Function *function = function;
        Type type;

        while (inPipe->Remove(&rec)) {
                // get value and type of the particular attribute to be summed
                type = function->Apply(rec, intAttrVal, doubleAttrVal);
                if (type == Int) {
                        intSum += intAttrVal;
                } else {
                        doubleSum += doubleAttrVal;
                }
        }

        ostringstream result;
        string resultSum;
        Record resultRec;

        if (type == Int) {
                result << intSum;
                resultSum = result.str();
                resultSum.append("|");

                Attribute IA = {"int", Int};
                Schema out_sch("out_sch", 1, &IA);
                //resultRec.SuckNextRecord(&out_sch, resultSum.c_str());
        } else {
                result << doubleSum;
                resultSum = result.str();
                resultSum.append("|");

                Attribute DA = {"double", Double};
                Schema out_sch("out_sch", 1, &DA);
                //resultRec.SuckNextRecord(&out_sch, resultSum.c_str());
        }
        this->outPipe->Insert(&resultRec);
        this->outPipe->ShutDown();
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->function = &computeMe;
	pthread_create(&thread, NULL, sum, this);
}

void Sum::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void Sum::Use_n_Pages(int n) {
	this->nPages = n;
}

//-----------------------------------DUPLICATE REMOVAL--------------------------------

void *DuplicateRemoval::duplicateRemoval(void *arg) {
	DuplicateRemoval *dr = (DuplicateRemoval *) arg;
	dr->DoDuplicateRemoval();
	return NULL;
}

void DuplicateRemoval::DoDuplicateRemoval() {

// get OrderMaker for all Attributes in the schema.

	OrderMaker *om = new OrderMaker(this->mySchema);

	cout << "Duplicate Removal on OrderMaker\n";

	// create a BigQ instance to Sort the input.

	Pipe *sortPipe = new Pipe(PIPE_SIZE);

	BigQ *bigQ = new BigQ(*(this->inPipe), *sortPipe, *om, this->nPages);

	ComparisonEngine comp;

	Record *temp = new Record;
	Record *check = new Record;


// get output from BigQ.

	if(sortPipe -> Remove(temp)) {
		//insert the first one
		bool more = true;
		while(more) {
			more = false;
			Record *copyMe = new Record();
			copyMe->Copy(temp);
			this->outPipe->Insert(copyMe);
			while(sortPipe->Remove(check)) {
				if(comp.Compare(temp, check, om) != 0) { //equal
					temp->Copy(check);
					more = true;
					break;
				}
			}
		}
	}
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {

	this->inPipe = &inPipe;

	this->outPipe = &outPipe;
 	this->mySchema = &mySchema;
	pthread_create(&thread, NULL, duplicateRemoval, this);
}

void DuplicateRemoval::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int n) {
	this->nPages = n;
}


//-----------------------------------GROUP BY---------------------------------------------------

void* GroupBy::groupBy(void *arg) {
	GroupBy *gb = (GroupBy *) arg;
	gb->DoGroupBy();
	return NULL;

}

void GroupBy::DoGroupBy() {

	cout << "Starting group by" << endl;

        Attribute DA = {"double", Double};
        Schema out_sch_double("out_sch", 1, &DA);
        
        Attribute IA = {"int", Int};
        Schema out_sch_int("out_sch", 1, &IA);

	int runlen = this->nPages;

        vector<int> groupByOrderAttrs;
        vector<Type> groupByOrderTypes;
        //params->groupbyOrder->GetOrderMakerAttrs(&groupByOrderAttrs, &groupByOrderTypes);

        int *projectAttrsToKeep = &groupByOrderAttrs[0];

        int numGroupByAttrs = groupByOrderAttrs.size();

        int mergeAttrsToKeep[1 + numGroupByAttrs];
        mergeAttrsToKeep[0] = 0;
        for (int i = 1; i <= numGroupByAttrs; i++) {
                mergeAttrsToKeep[i] = i - 1;
        }

        Pipe *bigqOutPipe = new Pipe(100);
        BigQ bigQ(*(this->inPipe), *(bigqOutPipe), *(this->groupbyOrder), runlen);

        ComparisonEngine comparator;
        int intAttrVal;
        double doubleAttrVal;
        Type type;
        int intSum = 0;
        double doubleSum = 0;
        Record currRec, nextRec;

        int currRecPresent = bigqOutPipe->Remove(&currRec);
        int nextRecPresent = currRecPresent;
        nextRec.Copy(&currRec);
        int numCurrRecAttrs = currRec.GetNumAtts();

        while (nextRecPresent) {

                int orderMakerAnswer = comparator.Compare(&currRec, &nextRec, this->groupbyOrder);
                
                // Perform summation until there is a mismatch
                if (orderMakerAnswer == 0) {

                        type = this->function->Apply(nextRec, intAttrVal, doubleAttrVal);

                        if (type == Int) {
                                intSum += intAttrVal;

                        } else {
                                doubleSum += doubleAttrVal;
                        }

                        nextRecPresent = bigqOutPipe->Remove(&nextRec);

                } 
                // create output tuple when there is a mismatch
                else {

                        ostringstream result;
                        string resultSum;
                        Record groupSumRec;

                        // create output record 
                        if (type == Int) {

                                result << intSum;
                                resultSum = result.str();
                                resultSum.append("|");
                                //groupSumRec.SuckNextRecord(&out_sch_int, resultSum.c_str());

                        } else {

                                result << doubleSum;
                                resultSum = result.str();
                                resultSum.append("|");
                                //groupSumRec.SuckNextRecord(&out_sch_double, resultSum.c_str());
                        }

                        // Get a record contaning only group by attributes
                        currRec.Project(projectAttrsToKeep, numGroupByAttrs, numCurrRecAttrs);

                        // merge the above record with the group Sum Record to get result record
                        Record resultRec;
                        resultRec.MergeRecords(&groupSumRec, &currRec, 1, numGroupByAttrs, mergeAttrsToKeep, 1 + numGroupByAttrs, 1);

                        this->outPipe->Insert(&resultRec);

                        currRec.Copy(&nextRec);
                        intSum = 0;
                        doubleSum = 0;
                }
        }

        ostringstream result;
        string resultSum;
        Record groupSumRec;
        
        if(type == Int){
                 
                 result << intSum;
                 resultSum = result.str();
                 resultSum.append("|");
                 //groupSumRec.SuckNextRecord(&out_sch_int, resultSum.c_str());
                
        }
        else{
                
                result << doubleSum;
                resultSum = result.str();
                resultSum.append("|");
                //groupSumRec.SuckNextRecord(&out_sch_double, resultSum.c_str());         
        }
        
        // Get a record containing only group by attributes
        currRec.Project(projectAttrsToKeep, numGroupByAttrs, numCurrRecAttrs);

        // merge the above record with the group Sum Record to get result record
        Record resultRec;
        resultRec.MergeRecords(&groupSumRec, &currRec, 1, numGroupByAttrs, mergeAttrsToKeep, 1 + numGroupByAttrs, 1);

        this->outPipe->Insert(&resultRec);
        this->outPipe->ShutDown();}


void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupbyOrder = &groupAtts;
	this->function = &computeMe;
	pthread_create(&thread, NULL, groupBy, this);
}

void GroupBy::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void GroupBy::Use_n_Pages(int n) {
	this->nPages = n;
}

// ---------------------------------- WRITE OUT ----------------------------------------

void *WriteOut::writeOut(void *arg) {
	WriteOut *wo = (WriteOut *) arg;
	wo->DoWriteOut();
	return NULL;
}


void WriteOut::DoWriteOut() {
	Record *tmpRecord = new Record;
	ComparisonEngine comp;

	while (this->inPipe->Remove(tmpRecord)) {
		tmpRecord->Print(this->mySchema);
	}

	delete tmpRecord;
	fclose(this->outFile);

}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;
	pthread_create(&thread, NULL, writeOut, this);
}

void WriteOut::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages(int n) {
	this->nPages = n;
}
