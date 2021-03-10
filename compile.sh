#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $DIR
echo $DIR
mkdir -p build/release

g++ -O3 -std=c++11 sort.cc -o build/release/sort -fopenmp -pthread
#g++ -O3 -std=c++11 sort_base.cc -o build/release/sort_base -fopenmp -pthread