DBI PROJECT SETUP DETAILS
apt-get install bison
2. apt-get install flex
3. Modify line 34 in Main.cc to read from wherever you put your tables.
4. run "make main" and not just "make" to create main.


TPCH Data
1. Download TPCH source.
2. Go to root of folder after unpacking tpch.
3. export DSS_PATH="path where u want generated files"
4. export PATH_SEP="path seperator for the mahchine '/'"
5. edit makefile to specify the 'Machine' and 'Database' 
6. run - make (to generate binary)
7. run ./dbgen -s 0.01 (to generate .tbl files in DSS_PATH location with scale factor 0.01)
