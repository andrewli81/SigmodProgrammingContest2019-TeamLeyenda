//
// Created by andrew on 4/27/19.
//

#ifndef JVM_OVERFLOW_SMALLBUFFERSCATTER_H
#define JVM_OVERFLOW_SMALLBUFFERSCATTER_H

#include <string>
#include <vector>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include "blockingconcurrentqueue.h"
#include "defs.h"
using namespace std;

class SmallBufferScatter {
private:

    INLINE void writeRecord(uint64_t& i, uint64_t& n, const uint64_t& globalOffset, vector<char*>& writeBuffer,
            char*& keyStore, const uint64_t& keyStoreOffset, char*& valueStore) {
        auto ptr = keyStore + keyStoreOffset + i * KEY_SIZE + REAL_KEY_SIZE;
        memcpy(&n, ptr, 4);
        uint64_t newOffset = offset + i + (keyStoreOffset / KEY_SIZE);
        memcpy(ptr, &newOffset, 4);
        uint64_t whichPartition = (globalOffset +  i * TUPLE_SIZE) / partitionSize;
        uint64_t partitionOffset = (globalOffset + i * TUPLE_SIZE) % partitionSize;
        copy(valueStore + n * TUPLE_SIZE,
             valueStore + (n + 1) * TUPLE_SIZE,
             writeBuffer[whichPartition] + partitionOffset);
        i++;
    }

    void writeRange(
            char*& keyStore,
            char*& valueStore,
            vector<char*>& writeBuffer,
            const uint64_t globalOffset,
            const uint64_t partitionSizePerThread) {
        auto keyStoreOffset = (globalOffset / TUPLE_SIZE) * KEY_SIZE;
        auto tupleCount = partitionSizePerThread / TUPLE_SIZE;
        uint64_t n = 0;
        uint64_t iters = tupleCount / 4;
        uint64_t remainder = tupleCount % 4;
        uint64_t i = 0;
        switch (remainder) {
            case 3:
                writeRecord(i, n, globalOffset, writeBuffer, keyStore, keyStoreOffset, valueStore);
            case 2:
                writeRecord(i, n, globalOffset, writeBuffer, keyStore, keyStoreOffset, valueStore);
            case 1:
                writeRecord(i, n, globalOffset, writeBuffer, keyStore, keyStoreOffset, valueStore);
        }
        for (int iter = 0; iter < iters; iter++) {
            writeRecord(i, n, globalOffset, writeBuffer, keyStore, keyStoreOffset, valueStore);
            writeRecord(i, n, globalOffset, writeBuffer, keyStore, keyStoreOffset, valueStore);
            writeRecord(i, n, globalOffset, writeBuffer, keyStore, keyStoreOffset, valueStore);
            writeRecord(i, n, globalOffset, writeBuffer, keyStore, keyStoreOffset, valueStore);
        }
//        delete[] l2Cache;
    }

    uint64_t numThreads;
    uint64_t partitionSize;
    uint64_t partitionNum;
    uint64_t size;
    uint64_t offset;
public:
    SmallBufferScatter(uint64_t numThreads, uint64_t partitionNum, uint64_t partitionSize, uint64_t offset) :
            numThreads(numThreads), partitionNum(partitionNum), partitionSize(partitionSize), offset(offset) {
        size = partitionSize * partitionNum;
    }

    void write(char*& keyStore, char*& valueStore, vector<char*>& smallBuffers) {
        //auto write_file_start = chrono::high_resolution_clock::now();
        // 100 MB
        uint64_t partitionSizePerThread = size / numThreads;

        vector<thread> ioThreads;
        for (size_t i = 0; i < numThreads; i++) {
            uint64_t globalOffset = i * partitionSizePerThread;
            ioThreads.emplace_back(&SmallBufferScatter::writeRange, this,
                                   ref(keyStore), ref(valueStore),
                                   ref(smallBuffers), globalOffset, partitionSizePerThread);
        }
        for (auto& th : ioThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        ioThreads.clear();
        //auto write_file_end = chrono::high_resolution_clock::now();
        //printTime("write file", write_file_start, write_file_end);
    }
};

#endif //JVM_OVERFLOW_SMALLBUFFERSCATTER_H
