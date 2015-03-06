CC = g++ -O2 -Wno-deprecated 
# Points to the root of fused Google Test, relative to where this file is.
FUSED_GTEST_DIR = ..

# Paths to the fused gtest files.
FUSED_GTEST_H = $(FUSED_GTEST_DIR)/gtest/gtest.h
FUSED_GTEST_ALL_CC = $(FUSED_GTEST_DIR)/gtest/gtest-all.cc

# Where to find gtest_main.cc.
GTEST_MAIN_CC = $(FUSED_GTEST_DIR)/gtest/gtest_main.cc

# Flags passed to the preprocessor.
# We have no idea here whether pthreads is available in the system, so
# disable its use.
CPPFLAGS += -I$(FUSED_GTEST_DIR) -DGTEST_HAS_PTHREAD=0

# Flags passed to the C++ compiler.
CXXFLAGS += -g

A2TESTS=TestSortedDBFile.cc
A2OBJECTS_TESTS=$(A2TESTS:.cc=.o)
A1TESTS=TestDBFile.cc
A1OBJECTS_TESTS=$(A1TESTS:.cc=.o)

tag = -i

ifdef linux
tag = -n
endif
	
main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o main.o -ll
	
a2test: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o Heap.o Sorted.o y.tab.o lex.yy.o a2test.o gtest-all.o gtest_main.o
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -pthread -o a2test Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o Heap.o Sorted.o y.tab.o lex.yy.o $(A2OBJECTS_TESTS) gtest-all.o gtest_main.o -o $@

a2test.o :
	for dir in $(A2TESTS); do \
		$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $$dir; \
	done

a1test: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o Heap.o Sorted.o y.tab.o lex.yy.o a1test.o gtest-all.o gtest_main.o
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -pthread -o a1test Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o Heap.o Sorted.o y.tab.o lex.yy.o $(A1OBJECTS_TESTS) gtest-all.o gtest_main.o -o $@

a1test.o :
	for dir in $(A1TESTS); do \
		$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $$dir; \
	done

main.o: main.cc
	$(CC) -g -c main.cc
	
Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Heap.o: Heap.cc
	$(CC) -g -c Heap.cc

Sorted.o: Sorted.cc
	$(CC) -g -c Sorted.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

gtest-all.o : $(FUSED_GTEST_H) $(FUSED_GTEST_ALL_CC)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(FUSED_GTEST_DIR)/gtest/gtest-all.cc

gtest_main.o : $(FUSED_GTEST_H) $(GTEST_MAIN_CC)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(GTEST_MAIN_CC)

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	rm -rf a1test
	rm -rf a2test
	rm -rf main
