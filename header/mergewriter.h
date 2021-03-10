#include <utility>

//
// Created by Xueqing Li on 2019-04-25.
//

#ifndef JVM_OVERFLOW_MERGEWRITER_H
#define JVM_OVERFLOW_MERGEWRITER_H

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <queue>
#include <unistd.h>
#include "blockingconcurrentqueue.h"
#include "defs.h"
using namespace std;

class MergeWriter {
private:
    string outfile;
    uint64_t numThreads;
    uint64_t size;
    uint64_t trunks;
    uint64_t trunkSize;
    uint64_t smallTrunkSize;
    uint64_t smallTrunkPerTrunk;
    uint64_t partitionSize = 2000000000;

    INLINE void writeRecord(uint64_t& l2Count, uint64_t& i, uint64_t& n, uint64_t& writeOffset,
                            char*& l2Buffer, char*& writeBuffer, char*& keyStore, const uint64_t& keyStoreOffset,
                            vector<vector<char*>>& valueStore, vector<uint64_t>& offsets) {
        if (UNLIKELY(l2Count == 2560)) {
            copy(l2Buffer, l2Buffer + 256000, writeBuffer + writeOffset);
            writeOffset += 256000;
            l2Count = 0;
        }
        auto ptr = keyStore + keyStoreOffset + i * KEY_SIZE + REAL_KEY_SIZE;
        memcpy(&n, ptr, 4);
        uint64_t offset = n * TUPLE_SIZE;
        uint64_t trunk = offset / trunkSize;
        uint64_t smallTrunk = (offset % trunkSize) / smallTrunkSize;
        uint64_t smallTrunkOffset = (offset % trunkSize) % smallTrunkSize;
        offsets[trunk]++;
        char* target = valueStore[trunk][smallTrunk] + smallTrunkOffset;
        copy(target,
             target + TUPLE_SIZE,
             l2Buffer + l2Count * TUPLE_SIZE);
        l2Count++;
        i++;
    }

    void writeRange(
            char*& keyStore,
            vector<vector<char*>>& valueStore,
            vector<vector<uint64_t>>& offsetsPerThread,
            uint64_t no,
            char* l2Cache,
            char* writeBuffer,
            const uint64_t globalOffset,
            const uint64_t partitionSizePerThread) {
        vector<uint64_t> offsets(trunks);
        char* writeBufferForTask = writeBuffer + globalOffset;
        auto keyStoreOffset = (globalOffset / TUPLE_SIZE) * KEY_SIZE;
        auto tupleCount = partitionSizePerThread/TUPLE_SIZE;
        uint64_t writeOffset = 0;
        uint64_t n = 0;
        uint64_t l2Count = 0;
        uint64_t iters = tupleCount / 4;
        uint64_t remainder = tupleCount % 4;
        uint64_t i = 0;

        switch (remainder) {
            case 3:
                writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore, offsets);
            case 2:
                writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore, offsets);
            case 1:
                writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore, offsets);
        }
        for (int iter = 0; iter < iters; iter++) {
            writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore, offsets);
            writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore, offsets);
            writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore, offsets);
            writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore, offsets);
        }
        if (l2Count > 0) {
            copy(l2Cache,
                 l2Cache + TUPLE_SIZE * l2Count,
                 writeBufferForTask + writeOffset);
        }
        offsetsPerThread[no] = offsets;
//        delete[] l2Cache;
    }


public:
    MergeWriter(
            string outfile,
            uint64_t numThreads,
            uint64_t size,
            uint64_t trunks,
            uint64_t trunkSize,
            uint64_t smallTrunkSize,
            uint64_t smallTrunkPerTrunk) :
            outfile(std::move(outfile)),
            numThreads(numThreads),
            size(size),
            trunks(trunks),
            trunkSize(trunkSize),
            smallTrunkSize(smallTrunkSize),
            smallTrunkPerTrunk(smallTrunkPerTrunk) {
//        cout << "threads: " << numThreads << endl;
//        cout << "size " << size << endl;
//        cout << "trunks " << trunks << endl;
//        cout << "trunkSize " << trunkSize << endl;
//        cout << "smallTrunkSize " << smallTrunkSize << endl;
//        cout << "smallTrunkPerTrunk " << smallTrunkPerTrunk << endl;
    }
    /*
     * For dataBuffer 1-5, 3 GB each
     * For dataBuffer6, 10 GB
     * 1. Start writing dataBuffers
     * 1. Start 5 threads to
     */
    void write(
            vector<vector<char*>>& data, char*& keys) {
        uint64_t tuplePerSmallTrunk = smallTrunkSize / TUPLE_SIZE;
        int fd = open(outfile.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);

        assert(fd != -1);
        assert(ftruncate(fd, size) != -1);

        auto writeFile = mmap(nullptr, size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
        assert(writeFile != (caddr_t) -1);
        char* writeBuffer = static_cast<char*> (writeFile);

        vector<uint64_t> offsets(trunks, 0);
        vector<uint64_t> deleted(trunks, 0);
        //auto sa_start = chrono::high_resolution_clock::now();

        RadixSorter sorter = RadixSorter(THREADS, true, 32, 95);

        uint64_t totalTuples = (size / TUPLE_SIZE);
        char *tmp = new char[totalTuples * KEY_SIZE];
        sorter.startSort(keys, tmp, totalTuples);
        delete[] tmp;

        //auto sa_end = chrono::high_resolution_clock::now();
        //printTime("sort again", sa_start, sa_end);

        uint64_t partitionSizePerThread = partitionSize / numThreads;
        uint64_t partitionNum = size / partitionSize;

        vector<thread> ioThreads;
        char* l2Cache = new char[256000 * numThreads];
        vector<vector<uint64_t>> offsetsPerThread(numThreads, vector<uint64_t>(trunks));

        for(uint64_t partition = 0; partition < partitionNum; partition++) {
            //auto write_to_memory_start = chrono::high_resolution_clock::now();
            //auto sc_start = chrono::high_resolution_clock::now();
            for (uint64_t i = 0; i < numThreads; i++) {
                uint64_t partitionOffset = i * partitionSizePerThread;
                uint64_t globalOffset = partition * partitionSize + partitionOffset;
                ioThreads.emplace_back(&MergeWriter::writeRange, this,
                                       ref(keys), ref(data), ref(offsetsPerThread), i, l2Cache + i * 256000,
                                       writeBuffer, globalOffset, partitionSizePerThread);
            }
            for (auto& th : ioThreads) {
                if (th.joinable()) {
                    th.join();
                }
            }
            //cout << "partition " << partition << " finished" << endl;
            ioThreads.clear();
            //auto sc_end = chrono::high_resolution_clock::now();
            //printTime("scatter", sc_start, sc_end);
            //auto bk_start = chrono::high_resolution_clock::now();
            for (uint64_t i = 0; i < trunks; i++) {
                for (uint64_t j = 0; j < numThreads; j++) {
                    offsets[i] += offsetsPerThread[j][i];
                }
                uint64_t deletions = offsets[i] / tuplePerSmallTrunk;
                for (uint64_t d = 0; d < deletions; d++) {
                    delete[] data[i][d + deleted[i]];
                }
                deleted[i] += deletions;
                offsets[i] = 0;
            }
            //auto bk_end = chrono::high_resolution_clock::now();
            //printTime("bookkeeping", bk_start, bk_end);
            //auto write_to_memory_end = chrono::high_resolution_clock::now();
            //printTime("scatter partition", write_to_memory_start, write_to_memory_end);
        }
    }
};

#endif //JVM_OVERFLOW_MERGEWRITER_H
