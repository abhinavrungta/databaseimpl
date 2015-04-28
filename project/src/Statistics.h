#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <string.h>
#include <map>
#include <iostream>

using namespace std;

class Statistics {

	bool isCalledFromApply;
	bool isApply; 

	std::map <string, int> *relationData;
	std::map <string, map <string, int> > *attrData; 	
public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
