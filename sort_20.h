#ifndef JVM_OVERFLOW_SORT20_H
#define JVM_OVERFLOW_SORT20_H

#include <iostream>
#include <iostream>
#include <vector>
#include <thread>
#include <fstream>
#include <algorithm>
#include <string>
#include <cstring>
#include <fcntl.h>
#include "header/radixsorter.h"
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include "header/preprocessingreader.h"
#include "header/postprocessingwriter.h"

using namespace std;

INLINE void sort_20(
        const string &infile, 
        const string &outfile, 
        const int64_t size) {
    const auto numTuples = static_cast<const uint64_t>(size / TUPLE_SIZE);
    //----------------READ FR FILE PART-------------------//
    //----------------READ FR FILE PART-------------------//
    //----------------READ FR FILE PART-------------------//
    //----------------READ FR FILE PART-------------------//
    //----------------READ FR FILE PART-------------------//
    //----------------READ FR FILE PART-------------------//
    char* data = new char[size];
    char* keys = new char[numTuples * KEY_SIZE];
    auto read_file_start = chrono::high_resolution_clock::now();
    PreProcessingReader(infile, THREADS, (uint64_t) size).read(keys, data);
    auto read_file_stop = chrono::high_resolution_clock::now();
    printTime("read file", read_file_start, read_file_stop);
    //---------------------SORT PART-----------------------//
    //---------------------SORT PART-----------------------//
    //---------------------SORT PART-----------------------//
    //---------------------SORT PART-----------------------//
    //---------------------SORT PART-----------------------//
    //---------------------SORT PART-----------------------//
    auto sort_start = chrono::high_resolution_clock::now();
    RadixSorter sorter = RadixSorter(THREADS, false, 32, 95);
    char* tmp = new char[numTuples * KEY_SIZE];
    sorter.startSort(keys, tmp, numTuples);
    delete[] tmp;
    auto sort_end = chrono::high_resolution_clock::now();
    printTime("sort file", sort_start, sort_end);
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    auto write_file_start = chrono::high_resolution_clock::now();
    PostProcessingWriter(outfile, THREADS, size, 250000000).write(keys, data);
    auto write_file_stop = chrono::high_resolution_clock::now();
    printTime("write file", write_file_start, write_file_stop);
}

#endif
