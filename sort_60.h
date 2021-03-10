#ifndef JVM_OVERFLOW_SORT60_H
#define JVM_OVERFLOW_SORT60_H

#include <iostream>
#include <iostream>
#include <vector>
#include <thread>
#include <fstream>
#include <algorithm>
#include <string>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include "header/radixsorter.h"
#include "header/trunkreader.h"
#include "header/trunkwriter.h"
#include "header/valuesorter.h"
#include "header/postprocessingwriter_60.h"
#include "header/postprocessingwriter.h"

using namespace std;

INLINE void sortTrunk(
        const string& infile,
        const uint64_t& trunks,
        const uint64_t& trunkSize,
        const uint64_t& memorySize,
        const uint64_t& numTuples,
        char*& keyData, char*& trunkData, char*& keyData2, char*& trunkData2, char*& memoryData) {
    atomic<bool> dataReady(true);

    moodycamel::BlockingConcurrentQueue<Data> sortWriteQueue(trunks);

    thread sortWriter([&]() {
        for (uint64_t i = 0; i < trunks; i++) {
            Data data;
            sortWriteQueue.wait_dequeue(data);

            //---------------------SORT PART-----------------------//
            //auto sort_start = chrono::high_resolution_clock::now();
            RadixSorter sorter = RadixSorter(THREADS, true, 0, 256);

            char *tmp = new char[numTuples * KEY_SIZE];
            sorter.startSort(data.keyData, tmp, numTuples);
            //auto sort_end = chrono::high_resolution_clock::now();
            delete[] tmp;

            //printTime("sort trunk", sort_start, sort_end);

            //----------------WRITE TO FILE PART-------------------//
            //auto write_file_start = chrono::high_resolution_clock::now();
            if (i < trunks - 1) {
                auto memoryBuffer = memoryData + i * memorySize;

                /*
                 * #define TEMP_FILE_NAME1 "/input-disk/temp_file1"
                 * #define TEMP_FILE_NAME2 "/input-disk/temp_file2"
                 * #define TEMP_FILE_NAME3 "/input-disk/temp_file3"
                 * #define TEMP_FILE_NAME4 "/input-disk/temp_file4"
                 * #define TEMP_FILE_NAME5 "/input-disk/temp_file5"
                 */
                TrunkWriter(TEMP_FILE_NAME + to_string(i), THREADS, trunkSize, memorySize, 500000000).write(data.keyData, data.trunkData,
                                                                                      memoryBuffer);
                dataReady = false;
            }
            //auto write_file_stop = chrono::high_resolution_clock::now();
            //printTime("write trunk", write_file_start, write_file_stop);
        }
    });

    for (uint64_t i = 0; i < trunks; i++) {
        char *keys;
        char *data;
        if (i % 2 == 0) {
            keys = keyData;
            data = trunkData;
        } else {
            keys = keyData2;
            data = trunkData2;
        }
        //auto read_file_start = chrono::high_resolution_clock::now();
        TrunkReader(infile, THREADS, i * trunkSize, trunkSize).read(keys, data);
        //auto read_file_stop = chrono::high_resolution_clock::now();
        //printTime("read trunk", read_file_start, read_file_stop);

        sortWriteQueue.enqueue(Data(keys, data));

        if (UNLIKELY(i == 0 || (i == trunks - 1))) {
            continue;
        } else {
            while (dataReady.load()) {
            }
            dataReady = true;
        }
    }
    sortWriter.join();
}

INLINE void sort_60(
        const string &infile, 
        const string &outfile, 
        const int64_t size) {

    const auto trunkSize = 5000000000;
    const auto memorySize = 1500000000;
    const uint64_t trunks = (size / trunkSize);
    const uint64_t numTuples = trunkSize / TUPLE_SIZE;

//    char* bufferedData (sorted, 1.5GB) -> "/input-disk/temp_file0" sorted 3.5GB
//    char* bufferedData + memorySize (sorted, 1.5GB)     ->  "/input-disk/temp_file1" sorted 1.5GB
//    char* bufferedData + memorySize * 2 (sorted, 1.5GB) ->  "/input-disk/temp_file2" sorted 1.5GB
//    char* bufferedData + memorySize * 3 (sorted, 1.5GB) ->  "/input-disk/temp_file3" sorted 1.5GB
//    char* bufferedData + memorySize * 4 (sorted, 1.5GB) ->  "/input-disk/temp_file4" sorted 1.5GB
//    ...
//    char* bufferedData + memorySize * 9 (sorted, 1.5GB) ->  "/input-disk/temp_file4" sorted 1.5GB

//    char* data (unsorted, need to sort using valuesorter.h with keys)
//    char* data2 (unsorted, need to sort using valuesorter.h with keys)
//    ValueSorter(THREADS, 500000000 (data size to sort)).sort(
//            keys, data, memoryData, 1000000000 (valueStore offset));

    char *bufferedData;
    if (trunks > 1) {
        bufferedData = new char[memorySize * (trunks - 1)];
    } else {
        bufferedData = nullptr;
    }
    char* data = new char[trunkSize];
    char* data2 = new char[trunkSize];
    char* keys = new char[KEY_SIZE * (trunkSize / TUPLE_SIZE)];
    char* keys2 = new char[KEY_SIZE * (trunkSize / TUPLE_SIZE)];

    auto all_start = chrono::high_resolution_clock::now();
    sortTrunk(
            infile,
            trunks,
            trunkSize,
            memorySize,
            numTuples,
            keys, data, keys2, data2, bufferedData);

    ValueSorter(THREADS, trunkSize).valueScatter(keys2, data2, data);

    delete[] keys;
    delete[] keys2;
    delete[] data2;

    auto all_end = chrono::high_resolution_clock::now();

    printTime("all", all_start, all_end);

    /*
     * Sort 10GB 'data' via GNU parallel sort
     */
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    auto write_file_start = chrono::high_resolution_clock::now();
    PostProcessingWriter60(infile, outfile, THREADS, size, memorySize).write(trunks, trunkSize, memorySize, bufferedData, data);
    auto write_file_stop = chrono::high_resolution_clock::now();
    printTime("merge file", write_file_start, write_file_stop);
}
#endif