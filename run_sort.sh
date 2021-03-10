#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
#echo 1 > /proc/sys/vm/drop_caches
#ls /output-disk/
#rm -rf /output-disk/*
#ls /output-disk/
${DIR}/build/release/sort $1 $2

