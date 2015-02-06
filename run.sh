#!/bin/bash
export dbfile=$(pwd)/dbfile/
mkdir -p ${dbfile}
export tpch=$(pwd)/db/
cd project/src/
make test
./test "$@"
