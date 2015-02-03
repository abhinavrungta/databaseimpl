//============================================================================
// Name        : DBImpl.cpp
// Author      : Abhinav
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#include <iostream>
using namespace std;

bool isPrime(int n) {
	return true;
}

#define BOOL char
#define FALSE 0
#define TRUE 1

void vars() {
	int a = 0, b = 1, c = 2, d = 3, e = 4;
	a = b - c + d * e;
	printf("%d\n", a); /* will print 1-2+3*4 = 11 */
}

void arrays() {
	int numbers[10];
	/* populate the array */
	numbers[0] = 10;
	numbers[1] = 20;

	printf("The 7th number in the array is %d", numbers[1]);
}

void strings() {
	char name[] = "John Smith";
	//char * name = "John Smith";
	int age = 27;

	/* prints out 'John Smith is 27 years old.' */
	printf("%s is %d years old.\n", name, age);

	if (strncmp(name, "John", 4) == 0) {
		printf("Hello, John!\n");
	} else {
		printf("You are not John. Go away.\n");
	}
}

void whileloop() {
	int n = 0;
	while (n < 10) {
		n++;

		/* check that n is odd */
		if (n % 2 == 1) {
			/* go back to the start of the while block */
			continue;
		}

		/* we reach this code only if n is even */
		printf("The number %d is even.\n", n);
	}
}

void pointers() {
	/* define a local variable a */
	int a = 1;

	/* define a pointer variable, and point it to a using the & operator */
	int * pointer_to_a = &a;

	printf("The value a is %d\n", a);
	printf("The value of a is also %d\n", *pointer_to_a);
}

typedef struct {
	char * brand;
	int model;
} vehicle;

int foo(int bar);

int foo(int bar) {
	return bar + 1;
}

void addone(int * n) {
	(*n)++;
}

typedef struct {
	char * name;
	char age;
} person;

int main() {
	cout << "!!!Hello World!!!" << endl; // prints !!!Hello World!!!
	vehicle v;
	v.model = 10;
	v.brand = "Hi";
	int n = 10;
	printf("Before: %d\n", n);
	addone(&n);
	printf("After: %d\n", n);

	person* myperson = new person();
	myperson->name = "John";
	myperson->age = 27;
	free(myperson);
	vars();

	char *bits = "123456";
	printf("%d", ((int*) bits)[0]);

	return 0;
}
