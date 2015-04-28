#!/bin/bash
export dbfile=$(pwd)/dbfile/
mkdir -p ${dbfile}
export tpch=$(pwd)/db/
cd project/src/
make clean
make a3test
./a3test "$@"
