#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <string>
#include <map>
#include <list>

using namespace std;

class Statistics {

	map<string, int> *relationData;			// <relation name, number of Tuples>
	map<string, map<string, int> > *attrData;// <relation name, map of attributes in relation with distinct count>
	map<string, list<string> > *partitions; // <partition name, list of Relations in partition>.
	bool isApply;

public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	void RemoveRel(string name);

	void Read(char *fromWhere);
	void Write(char *fromWhere);
	void Print();

	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	int GetPartitionName(char *relNames[], int numToJoin, string &leftRel,
			string &rightRel);
	void LoadAllStatistics();
};
#endif
