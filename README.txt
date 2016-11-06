RUNNING TESTS
1. TO RUN THE TEST. JUST EXECUTE ‘run.sh’.
2. Test #6 requires CNF to be entered. Sample CNF for lineitem table - (l_orderkey < 29)
Note: By Default I will be working on linetime table.
You can uncomment lines 87 - 98 in TestDBFile to allow the user to select a db for each test.

What does run.sh do -
1. sets the path for tpch files (./db/) and generated DBFiles (./dbfile/)
2. make test.
3. runs the test.

FOLDER STRUCTURE -
1. Google Test Framework is used for testing. Tests are included in TestDBFile.cc
2. TPCH FILES ARE IN FOLDER ‘DB’
3. SOURCE AND TEST FILES ARE IN FOLDER ‘PROJECT’.
4. ALL FILES STARTING WITH NAME TEST ARE ‘TEST FILES’.


DEPENDENCIES
1. apt-get install bison
2. apt-get install flex
