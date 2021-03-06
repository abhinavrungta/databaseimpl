#include "Statistics.h"
#include <iostream>
#include <map>
#include <set>
#include <stdlib.h>
#include <fstream>
#include <math.h>
#include <string.h>

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
		// if added relation above for first time in relationData, create a partition also.
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

	//copy relation attributes
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

// Remove all info for Relation
void Statistics::RemoveRel(string relName) {
	relationData->erase(relName);
	attrData->erase(relName);
	partitions->erase(relName);
}

// Read Stats from File.
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

// Write Stats to File.
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

int Statistics::GetRelation(string &attr, string &rel) {
	int prefixPos = attr.find(".");
	if (prefixPos != string::npos) {
		rel = attr.substr(0, prefixPos);
	} else {
		map<string, map<string, int> >::iterator mapEntry = attrData->begin();
		for (; mapEntry != attrData->end(); mapEntry++) {
			if ((*attrData)[mapEntry->first].count(attr) > 0) {
				rel = mapEntry->first;
				break;
			}
		}
		// Not Found.
		if (mapEntry == attrData->end()) {
			return 0;
		}
	}
	return 1;
}

// Searches and Sets the left and right partition in which the relNames are divided.
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
		// search the partition. if atleast one element not found, then loop to next partition above.
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
			// found all elements of this partition.
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
		// if all the relNames were not covered in the two partitions.
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

// Result Estimate for complex queries.
// Refer to http://codex.cs.yale.edu/avi/db-book/db6/slide-dir/PDF-dir/ch13.pdf
// Statistics For Cost Estimation -> Pages 1.36 to 1.48
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

	map<string, int> relOpMap; // Used to keep track of attributes in the query.
	bool isdependent = false;
	string prevOperand;
	string dependentOperandVal;
	int dependentOperandCtr = 1;

//And list is structured as a root, a orlist the left and andlist to the right.
//Or list is structured as a root, a comparison the left and orlist to the right.
//a comparison is structed as a root and operands to the left and right.
//operands consists of a code and value.
	while (currentAnd != NULL) {
		currentOr = currentAnd->left;
		resultORFactor = 1.0;

		while (currentOr != NULL) {
			ComparisonOp *currentCompOp = currentOr->left;

			// If we have an Orlist like this (A = a OR A = b OR A = c).
			// i.e the estimate for the count of attribute in the relation is dependent on other selection operations.
			if (strcmp((currentCompOp->left->value), prevOperand.c_str())
					== 0) {
				isdependent = true;
				dependentOperandCtr += 1;
				dependentOperandVal = currentCompOp->left->value;
			}
			prevOperand = currentCompOp->left->value;

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
					// cout << "Comes to T(R1)*T(R2)/max(V(R1,A),V(R2,A))" << endl;
					//find distinct counts of both attributes for the relations.
					double leftDistinctCount =
							(*attrData)[leftRelation][currentCompOp->left->value];
					double rightDistinctCount =
							(*attrData)[rightRelation][currentCompOp->right->value];

					resultORFactor *=
							(1.0
									- (1.0
											/ max(leftDistinctCount,
													rightDistinctCount)));
				} else if (currentCompOp->code == GREATER_THAN
						|| currentCompOp->code == LESS_THAN) {
					resultORFactor *= (2.0 / 3.0);		// 1.0 - (1.0 /3.0)
				}
			} else {
				// when rightOperand is INT, DOUBLE OR STRING.
				if (currentCompOp->code == EQUALS) {
					// cout << "Estimate is T(R)/V(R,A)" << endl;
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
		// when the attribute is dependent, the resultEstimate is T(R)*(1 - n/V(R,A)) instead of T(R)*(1 - 1/V(R,A))pow(n)
		// where n is number of distinct counts for A in the query.
		if (isdependent) {
			double factor = (1.0
					- (1.0 * dependentOperandCtr)
							/ (*attrData)[leftRelation][dependentOperandVal]);
			for (int i = 0; i < dependentOperandCtr; i++) {
				factor /=
						(1.0
								- (1.0
										/ (*attrData)[leftRelation][dependentOperandVal]));
			}
			resultORFactor *= factor;
			dependentOperandCtr = 1;
			dependentOperandVal = "";
		}
		isdependent = false;

		resultORFactor = 1.0 - resultORFactor;
		resultANDFactor *= resultORFactor;

		currentAnd = currentAnd->rightAnd;
	}

	// The relations are part of two partitions. Left and Right. Get the Relation Name for the partitions to retrieve Tuple Count for the Estimation.
	GetPartitionName(relNames, numToJoin, leftRelation, rightRelation);
	double numTuples = 1.0;
	map<string, int>::iterator relIter = relationData->begin();
	for (; relIter != relationData->end(); ++relIter) {
		if (leftRelation.compare(relIter->first) == 0
				|| rightRelation.compare(relIter->first) == 0) {
			numTuples = numTuples * relIter->second;
		}
	}
	// Get the Estimate by a product of the numTuples and Estimate Factor
	resultEstimate = numTuples * resultANDFactor;

	// Removes the Left and Right Relation, Adds a new joinedRel.
	// For the joinedRel -> numberTuples = Estimate. All attrbutes from lRel and rRel with updated distinctCounts.
	// Reference for DistinctCount Estimation : http://codex.cs.yale.edu/avi/db-book/db6/slide-dir/PDF-dir/ch13.pdf
	// Statistics For Cost Estimation -> Pages 1.36 to 1.48
	if (isApply) {
		map<string, int>::iterator distinctCountMapITR;
		string joinedRel = leftRelation;

		if (isJoinPerformed) {
			joinedRel += "_" + rightRelation;
		}

		// Update Attribute Data for joinedRel.
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

		// Update Relation Data for joinedRel.
		(*relationData)[joinedRel] = round(resultEstimate);

		// Update Partition Data for joinedRel. Merge the two Partitions.
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

		// Remove old Relations Data.
		RemoveRel(leftRelation);
		RemoveRel(rightRelation);
	}
	return resultEstimate;
}

void Statistics::LoadAllStatistics() {
	char *relName[] = { (char*) "supplier", (char*) "partsupp",
			(char*) "lineitem", (char*) "orders", (char*) "customer",
			(char*) "nation", (char*) "part", (char*) "region" };
	AddRel(relName[0], 10000);     //supplier
	AddRel(relName[1], 800000);    //partsupp
	AddRel(relName[2], 6001215);   //lineitem
	AddRel(relName[3], 1500000);   //orders
	AddRel(relName[4], 150000);    //customer
	AddRel(relName[5], 25);        //nation
	AddRel(relName[6], 200000);   //part
	AddRel(relName[7], 5);        //region

	AddAtt(relName[0], (char*) "s_suppkey", 10000);
	AddAtt(relName[0], (char*) "s_nationkey", 25);
	AddAtt(relName[0], (char*) "s_acctbal", 9955);
	AddAtt(relName[0], (char*) "s_name", 100000);
	AddAtt(relName[0], (char*) "s_address", 100000);
	AddAtt(relName[0], (char*) "s_phone", 100000);
	AddAtt(relName[0], (char*) "s_comment", 10000);

	AddAtt(relName[1], (char*) "ps_suppkey", 10000);
	AddAtt(relName[1], (char*) "ps_partkey", 200000);
	AddAtt(relName[1], (char*) "ps_availqty", 9999);
	AddAtt(relName[1], (char*) "ps_supplycost", 99865);
	AddAtt(relName[1], (char*) "ps_comment", 799123);

	AddAtt(relName[2], (char*) "l_returnflag", 3);
	AddAtt(relName[2], (char*) "l_discount", 11);
	AddAtt(relName[2], (char*) "l_shipmode", 7);
	AddAtt(relName[2], (char*) "l_orderkey", 1500000);
	AddAtt(relName[2], (char*) "l_receiptdate", 0);
	AddAtt(relName[2], (char*) "l_partkey", 200000);
	AddAtt(relName[2], (char*) "l_suppkey", 10000);
	AddAtt(relName[2], (char*) "l_linenumbe", 7);
	AddAtt(relName[2], (char*) "l_quantity", 50);
	AddAtt(relName[2], (char*) "l_extendedprice", 933900);
	AddAtt(relName[2], (char*) "l_tax", 9);
	AddAtt(relName[2], (char*) "l_linestatus", 2);
	AddAtt(relName[2], (char*) "l_shipdate", 2526);
	AddAtt(relName[2], (char*) "l_commitdate", 2466);
	AddAtt(relName[2], (char*) "l_shipinstruct", 4);
	AddAtt(relName[2], (char*) "l_comment", 4501941);

	AddAtt(relName[3], (char*) "o_custkey", 150000);
	AddAtt(relName[3], (char*) "o_orderkey", 1500000);
	AddAtt(relName[3], (char*) "o_orderdate", 2406);
	AddAtt(relName[3], (char*) "o_totalprice", 1464556);
	AddAtt(relName[3], (char*) "o_orderstatus", 3);
	AddAtt(relName[3], (char*) "o_orderpriority", 5);
	AddAtt(relName[3], (char*) "o_clerk", 1000);
	AddAtt(relName[3], (char*) "o_shippriority", 1);
	AddAtt(relName[3], (char*) "o_comment", 1478684);

	AddAtt(relName[4], (char*) "c_custkey", 150000);
	AddAtt(relName[4], (char*) "c_nationkey", 25);
	AddAtt(relName[4], (char*) "c_mktsegment", 5);
	AddAtt(relName[4], (char*) "c_name", 150000);
	AddAtt(relName[4], (char*) "c_address", 150000);
	AddAtt(relName[4], (char*) "c_phone", 150000);
	AddAtt(relName[4], (char*) "c_acctbal", 140187);
	AddAtt(relName[4], (char*) "c_comment", 149965);

	AddAtt(relName[5], (char*) "n_nationkey", 25);
	AddAtt(relName[5], (char*) "n_regionkey", 5);
	AddAtt(relName[5], (char*) "n_name", 25);
	AddAtt(relName[5], (char*) "n_comment", 25);

	AddAtt(relName[6], (char*) "p_partkey", 200000);
	AddAtt(relName[6], (char*) "p_size", 50);
	AddAtt(relName[6], (char*) "p_container", 40);
	AddAtt(relName[6], (char*) "p_name", 199996);
	AddAtt(relName[6], (char*) "p_mfgr", 5);
	AddAtt(relName[6], (char*) "p_brand", 25);
	AddAtt(relName[6], (char*) "p_type", 150);
	AddAtt(relName[6], (char*) "p_retailprice", 20899);
	AddAtt(relName[6], (char*) "p_comment", 127459);

	AddAtt(relName[7], (char*) "r_regionkey", 5);
	AddAtt(relName[7], (char*) "r_name", 5);
	AddAtt(relName[7], (char*) "r_comment", 5);
}
