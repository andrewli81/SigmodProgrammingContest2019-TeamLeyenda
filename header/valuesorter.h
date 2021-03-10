//
// Created by andrew on 4/22/19.
//

#ifndef JVM_OVERFLOW_VALUESORTER_H
#define JVM_OVERFLOW_VALUESORTER_H


#include <string>
#include <vector>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include "blockingconcurrentqueue.h"
#include "valuesorter.h"
#include "defs.h"
using namespace std;

class ValueSorter {
private:

    INLINE void writeRecord(uint64_t& l2Count, uint64_t& i, uint64_t& n, uint64_t& writeOffset,
                            char*& l2Buffer, char*& writeBuffer, char*& keyStore, const uint64_t& keyStoreOffset,
                            char*& valueStore) {
        if (UNLIKELY(l2Count == 2560)) {
            copy(l2Buffer, l2Buffer + 256000, writeBuffer + writeOffset);
            writeOffset += 256000;
            l2Count = 0;
        }
        auto ptr = keyStore + keyStoreOffset + i * KEY_SIZE + REAL_KEY_SIZE;
        memcpy(&n, ptr, 4);

        copy(valueStore + n * TUPLE_SIZE,
             valueStore + (n + 1) * TUPLE_SIZE,
             l2Buffer + l2Count * TUPLE_SIZE);
        l2Count++;
        i++;
    }

    void writeRange(
            char*& keyStore,
            char*& valueStore,
            char* l2Cache,
            char* writeBuffer,
            const uint64_t globalOffset,
            const uint64_t partitionOffset,
            const uint64_t partitionSizePerThread) {
        char* writeBufferForTask = writeBuffer + partitionOffset;
        auto keyStoreOffset = (globalOffset / TUPLE_SIZE) * KEY_SIZE;
        auto tupleCount = partitionSizePerThread/TUPLE_SIZE;
        //cout << "keyStoreOffset " << keyStoreOffset << endl;
        //cout << "tupleCount " << tupleCount << endl;
        uint64_t writeOffset = 0;
        uint64_t n = 0;
        uint64_t l2Count = 0;
        uint64_t iters = tupleCount / 4;
        uint64_t remainder = tupleCount % 4;
        uint64_t i = 0;

        switch (remainder) {
            case 3:
                writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore);
            case 2:
                writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore);
            case 1:
                writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore);
        }
        for (int iter = 0; iter < iters; iter++) {
            writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore);
            writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore);
            writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore);
            writeRecord(l2Count, i, n, writeOffset, l2Cache, writeBufferForTask, keyStore, keyStoreOffset, valueStore);
        }
        if (l2Count > 0) {
            copy(l2Cache,
                 l2Cache + TUPLE_SIZE * l2Count,
                 writeBufferForTask + writeOffset);
        }
    }

    uint64_t numThreads;
    uint64_t memoryBufferSize;
public:
    ValueSorter(uint64_t numThreads, uint64_t memoryBufferSize) :
            numThreads(numThreads), memoryBufferSize(memoryBufferSize) {
    }

    void valueScatter(char*& keyStore, char*& valueStore, char*& memoryBuffer) {
        vector<thread> ioThreads;
        //auto write_to_file_start = chrono::high_resolution_clock::now();

        char* l2Cache = new char[256000 * numThreads];

        uint64_t memoryBufferSizePerThread = memoryBufferSize / numThreads;

        for (size_t i = 0; i < numThreads; i++) {
            uint64_t partitionOffset = i * memoryBufferSizePerThread;
            ioThreads.emplace_back(&ValueSorter::writeRange, this,
                                    ref(keyStore), ref(valueStore), l2Cache + i * 256000,
                                    memoryBuffer, partitionOffset, partitionOffset, memoryBufferSizePerThread);
        }

        for (auto& th : ioThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        delete[] l2Cache;
        //auto write_to_file_end = chrono::high_resolution_clock::now();
        //printTime("sort value store", write_to_file_start, write_to_file_end);
    }
};


#endif //JVM_OVERFLOW_VALUESORTER_H
