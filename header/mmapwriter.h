//
// Created by andrew on 4/3/19.
//

#ifndef JVM_OVERFLOW_MMAPWRITER_H
#define JVM_OVERFLOW_MMAPWRITER_H

#include <string>
#include <vector>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include "concurrentqueue.h"
#include "defs.h"
using namespace std;

class MMapWriter {
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
            const uint64_t partitionSizePerThread) {
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
//        delete[] l2Cache;
    }


    string outfile;
    uint64_t numThreads;
    uint64_t partitionSize;
    uint64_t size;
public:
    MMapWriter(string outfile, uint64_t numThreads, uint64_t size, uint64_t partitionSize) :
            outfile(std::move(outfile)), numThreads(numThreads), size(size), partitionSize(partitionSize) {
    }

    void write(char*& keyStore, char*& valueStore) {
        auto write_file_start = chrono::high_resolution_clock::now();
        // 100 MB
        int fd = open(outfile.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);

        assert(fd != -1);
        assert(ftruncate(fd, size) != -1);

        auto writeFile = mmap(nullptr, size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
        assert(writeFile != (caddr_t) -1);

        vector<thread> ioThreads;
        //auto write_file_end = chrono::high_resolution_clock::now();
        //printTime("mmap", write_file_start, write_file_end);
        //auto write_to_file_start = chrono::high_resolution_clock::now();

        char* l2Cache = new char[256000 * numThreads];

        uint64_t memoryBufferSizePerThread = size / numThreads;

        for (size_t i = 0; i < numThreads; i++) {
            uint64_t partitionOffset = i * memoryBufferSizePerThread;
            ioThreads.emplace_back(&MMapWriter::writeRange, this,
                                   ref(keyStore), ref(valueStore), l2Cache + i * 256000,
                                   static_cast<char*>(writeFile), partitionOffset, memoryBufferSizePerThread);
        }

        for (auto& th : ioThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
    }
};


#endif

