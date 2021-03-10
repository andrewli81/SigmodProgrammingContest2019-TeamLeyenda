#ifndef JVM_OVERFLOW_SORT20_new_H
#define JVM_OVERFLOW_SORT20_new_H

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
#include "header/mergewriter.h"
#include "header/smallbufferscatter.h"
#include "header/postprocessingwriter_60.h"
#include "header/postprocessingwriter.h"

using namespace std;

INLINE void sortTrunk(
        const string& infile,
        const uint64_t& trunks,
        const uint64_t& trunkSize,
        const uint64_t& partitionSize,
        const uint64_t& partitionPerTrunk,
        const uint64_t& numTuples,
        char*& keyData, char*& trunkData, char*& trunkData2, vector<vector<char*>>& bufferedData) {
    atomic<bool> dataReady(true);

    moodycamel::BlockingConcurrentQueue<Data> sortWriteQueue(trunks);

    thread sortWriter([&]() {
        for (uint64_t i = 0; i < trunks; i++) {

            Data data;
            sortWriteQueue.wait_dequeue(data);

            //---------------------SORT PART-----------------------//

            RadixSorter sorter = RadixSorter(THREADS, true, 0, 256);

            char *tmp = new char[numTuples * KEY_SIZE];
            sorter.startSort(data.keyData, tmp, numTuples);
            delete[] tmp;


            //----------------WRITE TO FILE PART-------------------//
            //auto write_file_start = chrono::high_resolution_clock::now();
            for (uint64_t j = 0; j < partitionPerTrunk; j++) {
                bufferedData[i][j] = new char[partitionSize];
            }
            SmallBufferScatter(THREADS, partitionPerTrunk, partitionSize, i * numTuples)
            .write(data.keyData, data.trunkData, bufferedData[i]);

            dataReady = false;

            if (i == trunks - 2) {
                delete[] trunkData;
            }
        }
    });

    for (uint64_t i = 0; i < trunks; i++) {
        char *data;
        if (i % 2 == 0) {
            data = trunkData;
        } else {
            data = trunkData2;
        }

        char* key = keyData + i * numTuples * KEY_SIZE;
        TrunkReader(infile, THREADS, i * trunkSize, trunkSize).read(key, data);


        sortWriteQueue.enqueue(Data(key, data));

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

INLINE void sort_20_new(
        const string &infile, 
        const string &outfile, 
        const int64_t size) {

    const auto trunkSize = 1000000000;
    const auto partitionSize = 40000000;
    const uint64_t trunks = (size / trunkSize);
    const uint64_t partitionPerTrunk = trunkSize / partitionSize;
    const uint64_t numTuples = trunkSize / TUPLE_SIZE;

    char* data = new char[trunkSize];
    char* data2 = new char[trunkSize];
    char* keys = new char[KEY_SIZE * numTuples * trunks];
    //cout << trunkSize << endl;
    //cout << KEY_SIZE * numTuples * trunks << endl;
    vector<vector<char*>> bufferedData(trunks, vector<char*>(partitionPerTrunk));

    sortTrunk(
            infile,
            trunks,
            trunkSize,
            partitionSize,
            partitionPerTrunk,
            numTuples,
            keys, data, data2, bufferedData);
    delete[] data2;


    /*
     * Sort 10GB 'data' via GNU parallel sort
     */
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    MergeWriter(outfile, THREADS, size, trunks, trunkSize, partitionSize, partitionPerTrunk).write(bufferedData, keys);

}
#endif
