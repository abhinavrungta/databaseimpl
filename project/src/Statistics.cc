#include "Statistics.h"
#include <iostream>
#include <map>
#include <set>
#include <stdlib.h>
#include <fstream>
#include <sstream>
#include <math.h>
#include <string.h>
#include <iomanip>
#include <typeinfo>
#define _DEP

Statistics::Statistics() {
	isApply = false;
	relationData = new map<string, int>();
	attrData = new map<string, map<string, int> >();
	partitions = new map<string, list<string> >();
}

Statistics::Statistics(Statistics &copyMe) {
	relationData = new map<string, int>(*(copyMe.relationData));
	attrData = new map<string, map<string, int> >(*(copyMe.attrData));
	partitions = new map<string, list<string> >(*(copyMe.partitions));
}

Statistics::~Statistics() {
	delete relationData;
	delete attrData;
	delete partitions;
}

void Statistics::AddRel(char *relName, int numTuples) {
	string rel(relName);
	pair<map<string, int>::iterator, bool> ret = relationData->insert(
			pair<string, int>(rel, numTuples));

	if (ret.second) {
		// if added relation above for first time in relationData.
		list<string> relationsList;
		relationsList.push_back(rel);
		partitions->insert(pair<string, list<string> >(rel, relationsList));
	} else {
		cout << "Duplicate found";
		relationData->erase(ret.first);
		relationData->insert(pair<string, int>(rel, numTuples));
	}
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
	string aName(attName);
	string rname(relName);

	if (numDistincts == -1) {
		int numTuples = relationData->at(rname);
		(*attrData)[rname][aName] = numTuples;
	} else {
		(*attrData)[rname][aName] = numDistincts;
	}
}

void Statistics::CopyRel(char *_oldName, char *_newName) {
	string oldName(_oldName);
	string newName(_newName);

	//copy relation data
	int oldNumTuples = (*relationData)[oldName];
	(*relationData)[newName] = oldNumTuples;

	//copy relation attribute
	map<string, int> &oldattrData = (*attrData)[oldName];
	for (map<string, int>::iterator oldAttrInfo = oldattrData.begin();
			oldAttrInfo != oldattrData.end(); ++oldAttrInfo) {
		string newAtt = newName;
		newAtt += "." + oldAttrInfo->first;
		(*attrData)[newName][newAtt] = oldAttrInfo->second;
	}

	// create partition for relation
	list<string> relationsList;
	relationsList.push_back(newName);
	partitions->insert(pair<string, list<string> >(newName, relationsList));
}

void Statistics::RemoveRel(string relName) {
	relationData->erase(relName);
	attrData->erase(relName);
	partitions->erase(relName);
}

void Statistics::Read(char *fromWhere) {
	ifstream readFile;
	readFile.open(fromWhere);
	if (!readFile.is_open()) {
		cout << "FILE doest not exist";
		return;
	}

//1.read relationData size
	string input;
	readFile >> input;
	int relationDataSize = atoi(input.c_str());

//clear relation data
	relationData->clear();

//2.read actual relationdata map
	for (int i = 0; i < relationDataSize; i++) {
		readFile >> input;
		size_t splitAt = input.find_first_of("#");
		string part1 = input.substr(0, splitAt);
		string part2 = input.substr(splitAt + 1);

		int part2Int = atoi(part2.c_str());
		(*relationData)[part1] = part2Int;
	}

//3.skip over attrData map size
	readFile >> input;

//clear attrData map
	attrData->clear();

//4. read in actual attrData map
	string relName, attrName, distinctCount;
	readFile >> relName >> attrName >> distinctCount;

	while (!readFile.eof()) {
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
	writeFile.open(fileName.c_str());

//1.relation data size
	int relationDataSize = relationData->size();
	writeFile << relationDataSize << "\n";

//2. actual relation data map
	for (map<string, int>::iterator entry = relationData->begin();
			entry != relationData->end(); entry++) {

		const char *first = entry->first.c_str();
		int second = entry->second;

		writeFile << first << "#" << second << "\n";

	}

//3. attrData size
	int attrDataSize = attrData->size();
	writeFile << attrDataSize << "\n";

//4. actual attrData map
	for (map<string, map<string, int> >::iterator ii = attrData->begin();
			ii != attrData->end(); ++ii) {
		for (map<string, int>::iterator j = ii->second.begin();
				j != ii->second.end(); ++j) {
			const char *first = (*ii).first.c_str();
			const char *second = (*j).first.c_str();
			int third = (*j).second;
			writeFile << first << " " << second << " " << third << "\n";
		}
	}
	writeFile.close();
}

void Statistics::Print() {
//1.relation data size
	int relationDataSize = relationData->size();
	cout << relationDataSize << "\n";

//2. actual relation data map
	for (map<string, int>::iterator entry = relationData->begin();
			entry != relationData->end(); entry++) {

		const char *first = entry->first.c_str();
		int second = entry->second;

		cout << first << "#" << second << "\n";

	}

//3. attrData size
	int attrDataSize = attrData->size();
	cout << attrDataSize << "\n";

//4. actual attrData map
	for (map<string, map<string, int> >::iterator ii = attrData->begin();
			ii != attrData->end(); ++ii) {
		for (map<string, int>::iterator j = ii->second.begin();
				j != ii->second.end(); ++j) {
			const char *first = (*ii).first.c_str();
			const char *second = (*j).first.c_str();
			int third = (*j).second;
			cout << first << " " << second << " " << third << "\n";
		}
	}
}

int Statistics::GetPartitionName(char *relNames[], int numToJoin,
		string &leftRel, string &rightRel) {
	map<string, list<string> >::iterator iterP;
	list<string> subset;
	list<string>::iterator iterSub;
	bool foundthis = false, atleastOneNotFound = false;
	bool leftAssigned = false;
	int numRels = 0;

	for (iterP = partitions->begin(); iterP != partitions->end(); ++iterP) {
		atleastOneNotFound = false;

		subset = iterP->second;
		// search the partition. if atleast one element not found, then loop to next above.
		for (iterSub = subset.begin(); iterSub != subset.end(); ++iterSub) {
			foundthis = false;
			for (int i = 0; i < numToJoin; i++) {
				if (string(relNames[i]).compare(*iterSub) == 0) {
					foundthis = true;
					break;
				}
			}
			if (!foundthis) {
				atleastOneNotFound = true;
				break;
			}
		}		//end for - inner loop
		if (!atleastOneNotFound) {
			// found all elements of this subset.
			if (!leftAssigned) {
				leftRel = iterP->first;
				leftAssigned = true;
				numRels += partitions->at(leftRel).size();
			} else {
				rightRel = iterP->first;
				numRels += partitions->at(rightRel).size();
			}
		}
	} // end for- outer loop
	if (numRels != numToJoin) {
		return -1;
	}
	return 1;
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[],
		int numToJoin) {
	isApply = true;
	Estimate(parseTree, relNames, numToJoin);
	isApply = false;
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames,
		int numToJoin) {

	double resultEstimate = 0.0;
	struct AndList *currentAnd;
	struct OrList *currentOr;

	currentAnd = parseTree;

	string leftRelation;
	string rightRelation;

	bool isJoinPerformed = false;

	double resultANDFactor = 1.0;
	double resultORFactor = 1.0;

	map<string, int> relOpMap;

//And list is structured as a root, a orlist the left and andlist to the right.
//Or list is structured as a root, a comparison the left and orlist to the right.
//a comparison is structed as a root and operands to the left and right.
//operands consists of a code and value.
	while (currentAnd != NULL) {
		currentOr = currentAnd->left;
		resultORFactor = 1.0;

		while (currentOr != NULL) {
			ComparisonOp *currentCompOp = currentOr->left;

			// find relation of left attribute. first attribute has to be a name
			if (currentCompOp->left->code != NAME) {
				cout << "LEFT should be attribute name" << endl;
				return 0;
			} else {
				//find the relation where the attribute lies.
				for (map<string, map<string, int> >::iterator mapEntry =
						attrData->begin(); mapEntry != attrData->end();
						mapEntry++) {
					if ((*attrData)[mapEntry->first].count(
							currentCompOp->left->value) > 0) {
						leftRelation = mapEntry->first;
						break;
					}
				}
			}

			// if right is also a name, it is a join.
			if (currentCompOp->right->code == NAME) {
				isJoinPerformed = true;

				//find right relation
				for (map<string, map<string, int> >::iterator mapEntry =
						attrData->begin(); mapEntry != attrData->end();
						++mapEntry) {
					if ((*attrData)[mapEntry->first].count(
							currentCompOp->right->value) > 0) {
						rightRelation = mapEntry->first;
						break;
					}
				}

				if (currentCompOp->code == EQUALS) {
//					cout << "Comes to T(R1)*T(R2)/max(V(R1,A),V(R2,A))" << endl;
					//find distinct counts of both attributes for the relations.
					double leftDistinctCount =
							(*attrData)[leftRelation][currentCompOp->left->value];
					double rightDistinctCount =
							(*attrData)[rightRelation][currentCompOp->right->value];

					resultORFactor *=
							(1.0
									- (1.0
											/ max(leftDistinctCount,
													rightDistinctCount))); //ORFACTOR??
				} else if (currentCompOp->code == GREATER_THAN
						|| currentCompOp->code == LESS_THAN) {
					resultORFactor *= (2.0 / 3.0);		// 1.0 - (1.0 /3.0)
				}
			} else {
				if (currentCompOp->code == EQUALS) {
					resultORFactor *=
							(1.0
									- (1.0
											/ (*attrData)[leftRelation][currentCompOp->left->value]));
					relOpMap[currentCompOp->left->value] = currentCompOp->code;

				} else if (currentCompOp->code == GREATER_THAN
						|| currentCompOp->code == LESS_THAN) {
					resultORFactor *= (2.0 / 3.0);		// 1.0 - (1.0 /3.0)
					relOpMap[currentCompOp->left->value] = currentCompOp->code;
				}
			}
			currentOr = currentOr->rightOr;
		}

		resultORFactor = 1.0 - resultORFactor;
		resultANDFactor *= resultORFactor;
		currentAnd = currentAnd->rightAnd;
	}

	double numTuples = 1.0;
	cout << "Left Rel Before " << leftRelation << endl;
	cout << "Right Rel Before " << rightRelation << endl;
	GetPartitionName(relNames, numToJoin, leftRelation, rightRelation);
	cout << "Left Rel " << leftRelation << endl;
	cout << "Right Rel " << rightRelation << endl;
	map<string, int>::iterator relIter = relationData->begin();
	for (; relIter != relationData->end(); ++relIter) {
		if (leftRelation.compare(relIter->first) == 0
				|| rightRelation.compare(relIter->first) == 0) {
			numTuples = numTuples * relIter->second;
		}
	}
	resultEstimate = numTuples * resultANDFactor;
	cout << "Est " << resultEstimate << endl;

	if (isApply) {
		map<string, int>::iterator relOpMapITR, distinctCountMapITR;
		string joinedRel = leftRelation;
		if (isJoinPerformed) {
			joinedRel += "_" + rightRelation;
		}
		// iterate thru all attributes in leftRel and calculate new distinct count.
		for (distinctCountMapITR = (*attrData)[leftRelation].begin();
				distinctCountMapITR != (*attrData)[leftRelation].end();
				distinctCountMapITR++) {
			if (relOpMap.count(distinctCountMapITR->first) > 0) {
				int code = relOpMap[distinctCountMapITR->first];
				if (code == EQUALS) {
					(*attrData)[joinedRel][distinctCountMapITR->first] = 1;	// if the attribute was in cnf and with equal operator.
				} else if ((code == LESS_THAN) || (code == GREATER_THAN)) {
					// if the attribute was in cnf and with less than or greater than operator.
					(*attrData)[joinedRel][distinctCountMapITR->first] =
							(int) round(
									(double) (distinctCountMapITR->second)
											/ 3.0);
				}
			} else {
				// if the attribute was not in cnf.
				(*attrData)[joinedRel][distinctCountMapITR->first] = min(
						(int) round(resultEstimate),
						distinctCountMapITR->second);
			}
		}

		if (isJoinPerformed) {
			// iterate thru all attributes in rightRel and calculate new distinct count.
			for (distinctCountMapITR = (*attrData)[rightRelation].begin();
					distinctCountMapITR != (*attrData)[rightRelation].end();
					distinctCountMapITR++) {
				if (relOpMap.count(distinctCountMapITR->first) > 0) {
					int code = relOpMap[distinctCountMapITR->first];
					if (code == EQUALS) { // if the attribute was in cnf and with equal operator.
						(*attrData)[joinedRel][distinctCountMapITR->first] = 1;
					} else if ((code == LESS_THAN) || (code == GREATER_THAN)) {
						// if the attribute was in cnf and with less than or greater than operator.
						(*attrData)[joinedRel][distinctCountMapITR->first] =
								(int) round(
										(double) (distinctCountMapITR->second)
												/ 3.0);
					}
				} else {
					// if the attribute was not in cnf.
					(*attrData)[joinedRel][distinctCountMapITR->first] = min(
							(int) round(resultEstimate),
							distinctCountMapITR->second);
				}
			}
		}

		(*relationData)[joinedRel] = round(resultEstimate);

		list<string> relationsList;
		copy(partitions->at(leftRelation).begin(),
				partitions->at(leftRelation).end(),
				back_inserter(relationsList));
		if (isJoinPerformed) {
			copy(partitions->at(rightRelation).begin(),
					partitions->at(rightRelation).end(),
					back_inserter(relationsList));
		}
		partitions->insert(
				pair<string, list<string> >(joinedRel, relationsList));

		RemoveRel(leftRelation);
		RemoveRel(rightRelation);
	}
	return resultEstimate;
}
