#ifndef DEFS_H
#define DEFS_H

#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072
#define PIPE_SIZE 100
#define RUNLEN 500

enum Target {
	Left, Right, Literal
};
enum CompOperator {
	LessThan, GreaterThan, Equals
};
enum Type {
	Int, Double, String
};

#endif

