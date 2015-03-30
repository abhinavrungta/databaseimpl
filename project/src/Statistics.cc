#include "Statistics.h"
#include <math.h>
#include <iostram>
#include <set>
#include <fstream>
#include <sstream>


Statistics::Statistics() {
	isCalledFromApply = false;
	isApply = false;
	relationData = new map <string, int>();
	attrData = new map <string, map<string, int>>();
}


Statistics::Statistics(Statistics &copyMe) {
	relationData = new map<string, int>(*(copyMe.relationData));
	attrData = new map<string, map<string, int>> (*(copyMe.attrData));
}


Statistics::~Statistics() {
	delete relationData;
	delete attrData;
}

void Statistics::AddRel(char *relName, int numTuples) {
	string rel(relName);
	
	pair<map<string, int>::iterator, bool> ret = relationData->insert(pair<string, int>(rel, numTuples));

	if(ret.second == false){
		cout << "Duplicate" << "\n";		
		relationData->erase(ret.first);
		relationData->insert(pair<string, int>(rel, numTuples));
	}
}


void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
	string aName(attName);
	string rName(relName);
	
	if(numDistincts == -1){
		int numTuples = relationData->at(rname);
		    (*attrData)[rname][aName] = numTuples;
        }
	else {
                (*attrData)[rname][aName] = numDistincts;
        }
}


void Statistics::CopyRel(char *oldName, char *newName) {
	string oldName(oldName);
	string newName(newName);

	//copy the relation data

	int oldNumTuples = (*relationData)[oldName];
	(*relationData)[newName] = oldNameTuples;

	map<string, int> &oldattrData = (*attrData)[oldName];

        for (map<string, int>::iterator oldAttrInfo = oldattrData.begin(); oldAttrInfo != oldattrData.end(); ++oldAttrInfo) {       
		string newAtt = newName;
                newAtt += "." + oldAttrInfo->first;
                //cout << (*oldAttrInfo).first << ": " << (*oldAttrInfo).second << endl;
                (*attrData)[newName][newAtt] = oldAttrInfo->second;
        }
}

void Statistics::Read(char *fromWhere) {
	string fileName(fromWhere);
	ifstream iFile(fromWhere);
	
	if(!iFile){
		cout << "FILE does not exist" << "\n";
		return;
	}

	ifstream readFile;

	readFile.open(fileName.c_str(), ios::in);

	//read size of relation data

	string input;
	readFile >> input;
	int relationDataSize = atoi(input.c_str());
	
	// clear the relation data from the buffer

	relationData->clear();
	
	// read actual relation data as a map

	for(int i = 0; i < relationDataSize; i++){
		readFile >> input;
		
		size_t splitAt = input.find_first_of("#");
		string part1 = input.substr(0, splitAt);
		string part2 = input.substr(splitAt + 1);
		
		int part2Int = atoi(part2.c_str());
		(*relationData)[part1] = part2Int;
	}

	// commit mpa size and clear map

	readFile >> input;
	attrData->clear();

	//read in the input datamap

	string relName, attrName, distinctCount;
	readFile >> relName >> attrName >> distinctCount;
	
	while(!readFile.eof()){
		
		int distinctCountInt = atoi(distinctCount.c_str());
                (*attrData)[relName][attrName] = distinctCountInt;
                readFile >> relName >> attrName >> distinctCount;

        }

        readFile.close();

}

void Statistics::Write(char *fromWhere) {
	string fileName(fromWhere);
	remove(fromWhere);

	ofstream writeFile;
	writeFile.open(fileName.c_str(), ios::out);

	// get relation data size

	int relationDataSize = relationData->size();
	writeFile << relationDataSize << "\n";


	//actual relation data map
	for(map <string, int>::iterator entry = relationData->begin(); entry != relationData->end(); entry++){
		const char *first = entry->first.c_str();
		int second = entry->second();

		writeFile << first << "#" << second << "\n";
	}
	
	//attribute data size calculation
	
	int attrDataSize = attrData->size();
	writeFile << attrDataSize << "\n";

	//actual attribute data map

        for (map<string, map<string, int> >::iterator ii = attrData->begin(); ii != attrData->end(); ++ii) {

                for (map<string, int>::iterator j = ii->second.begin(); j != ii->second.end(); ++j) {
                        //cout << (*ii).first << " : " << (*j).first << " : " << (*j).second << endl;
			//1. rel name 2.attr name 3.distincts
                        const char *first = (*ii).first.c_str();
                        const char *second = (*j).first.c_str();
                        int third = (*j).second;
                        writeFile << first << " " << second << " " << third << "\n";
                }

        }

        writeFile.close();	


}

void Statistics::Apply(struct AndList *parseTree, char *relNames[],
		int numToJoin) {

	isCalledFromApply = true;
	isApply = true;
	Estimate(parseTree, relNames, numToJoin);
	isApply = false;
	isCalledFromApply = false;
}


double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {
/*
	double resEstimate = 0.0;
	
	struct AndList *currentAnd;	
	struct OrList *currentOr;
	
	currentAnd = parseTree;
	
	string LeftRelation;
	string RightRelation;

	string leftAttr;
	string rightAttr;
		
	string joinleftrelation, joinrightrelation;

	bool isJoin = false;
	bool isJoinPerformed = false;

	bool isDeep = false;
	bool done = false;
	string prev;

	double resultANDfactor = 1.0;
	double resultORfactor = 1.0;

	map<string, int> relOpmap
*/
}

