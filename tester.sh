#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

if [ ! -f "/input-disk/input_${2}" ]
then
    echo "Generating records: input_$2"
    cp gensort/gensort /input-disk
    cd /input-disk
    if [ "$2" == "10g" ]
    then
        ./gensort -a 100000000 input_$2
    elif [ "$2" == "20g" ]
    then
        ./gensort -a 200000000 input_$2
    elif [ "$2" == "60g" ]
    then
        ./gensort -s 600000000 input_$2
    fi
    rm gensort
    cd ..
    echo "Data input_$2 already exists!"
fi

echo "Work directory: $(pwd)"

echo "Running sort"
START_TIME=`echo $(($(date +%s%N)/1000000))`
${DIR}/build/release/sort "/input-disk/input_${2}" "/output-disk/output_${1}_${2}"
END_TIME=`echo $(($(date +%s%N)/1000000))`
echo "time: $(($END_TIME - $START_TIME)) milliseconds"

cp /src/gensort/valsort /output-disk
cd /output-disk
./valsort "output_${1}_${2}"
rm -f output*
cd /input-disk
rm -f temp*
