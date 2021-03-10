//
// Created by andrew on 4/3/19.
//

#ifndef TRUNKWRITER_H
#define TRUNKWRITER_H

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

class TrunkWriter {
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

    string outfile;
    uint64_t numThreads;
    uint64_t partitionSize;
    uint64_t memoryBufferSize;
    uint64_t size;
public:
    TrunkWriter(string outfile, uint64_t numThreads, uint64_t size, uint64_t memoryBufferSize, uint64_t partitionSize) :
    outfile(std::move(outfile)), numThreads(numThreads), size(size), memoryBufferSize(memoryBufferSize), partitionSize(partitionSize) {
    }

    void write(char*& keyStore, char*& valueStore, char*& memoryBuffer) {
        // 100 MB
        vector<thread> ioThreads1;
        char *l2Cache = new char[256000 * numThreads];

        uint64_t memoryBufferSizePerThread = memoryBufferSize / numThreads;
        for (size_t i = 0; i < numThreads; i++) {
            uint64_t partitionOffset = i * memoryBufferSizePerThread;
            ioThreads1.emplace_back(&TrunkWriter::writeRange, this,
                                    ref(keyStore), ref(valueStore), l2Cache + i * 256000,
                                    memoryBuffer, partitionOffset, partitionOffset, memoryBufferSizePerThread);
        }

        if (memoryBufferSize < size) {
            uint64_t partitionNum = (size - memoryBufferSize) / partitionSize;
            uint64_t partitionSizePerThread = partitionSize / numThreads;

            moodycamel::BlockingConcurrentQueue<char *> writeParition(partitionNum);

            // Paritition the write file
            ofstream of(outfile);
            atomic<bool> dataReady(true);
            thread consumer([&]() {
                setThreadAffinity(1);
                for (uint64_t i = 0; i < partitionNum; ++i) {
                    char *item;
                    writeParition.wait_dequeue(item);
                    //auto write_to_file_start = chrono::high_resolution_clock::now();
                    of.write(item, partitionSize);
                    dataReady = false;
                    //auto write_to_file_end = chrono::high_resolution_clock::now();
                    //printTime("write to file", write_to_file_start, write_to_file_end);
                }
            });

            char *writeBuffer = new char[partitionSize];
            char *writeBuffer2 = new char[partitionSize];
            char *l2Cache2 = new char[256000 * numThreads];

            vector<thread> ioThreads2;
            for (size_t partition = 0; partition < partitionNum; partition++) {
    //            cout << "partition " << partition << endl;
                char *bufferToUse;
                if (partition % 2 == 0) {
                    bufferToUse = writeBuffer;
                } else {
                    bufferToUse = writeBuffer2;
                }
                //auto write_to_memory_start2 = chrono::high_resolution_clock::now();
                for (size_t i = 0; i < numThreads; i++) {
                    uint64_t partitionOffset = i * partitionSizePerThread;
                    uint64_t globalOffset = memoryBufferSize + partition * partitionSize + partitionOffset;
    //                cout << "partition_offset " << partitionOffset << endl;
    //                cout << "globalOffset " << globalOffset << endl;
                    ioThreads2.emplace_back(&TrunkWriter::writeRange, this,
                                            ref(keyStore), ref(valueStore), l2Cache2 + i * 256000,
                                            bufferToUse,
                                            globalOffset, partitionOffset,
                                            partitionSizePerThread);
                }

                for (auto &th : ioThreads2) {
                    if (th.joinable()) {
                        th.join();
                    }
                }
                ioThreads2.clear();

                //auto write_to_memory_end2 = chrono::high_resolution_clock::now();
                //printTime("write to memory", write_to_memory_start2, write_to_memory_end2);

                writeParition.enqueue(bufferToUse);
                if (UNLIKELY(partition == 0 || (partition == (partitionNum - 1)))) {
                    continue;
                } else {
                    while (dataReady.load()) {
                    }
                    dataReady = true;
                }
            }
            if (consumer.joinable()) {
                consumer.join();
            }
            delete[] writeBuffer;
            delete[] writeBuffer2;
            delete[] l2Cache2;
        }
        for (auto &th : ioThreads1) {
            if (th.joinable()) {
                th.join();
            }
        }
        delete[] l2Cache;
    }
};


#endif //TRUNKWRITER_H
