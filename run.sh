#!/bin/bash
if [ -z "$1" ]
then
    echo "Usage: run.sh [small|medium|large]"
    exit 1
fi
echo "Running task: $1"
./compile.sh
START_TIME=`echo $(($(date +%s%N)/1000000000))`
./build/release/sort /input-disk/$1 /output-disk/$1.out
END_TIME=`echo $(($(date +%s%N)/1000000000))`
echo "time: $(($END_TIME - $START_TIME)) seconds"
./../gensort/valsort /output-disk/$1.out
rm -rf build
rm /output-disk/temp_file* /output-disk/$1.out
#sync; echo 1 > /proc/sys/vm/drop_caches 
