//
// Created by Zhaoxing Li on 2019-04-02.
//
#ifndef SIGMOD_PREPROCESSING_READER_H
#define SIGMOD_PREPROCESSING_READER_H
#include <string>
#include <vector>
#include <fstream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include "concurrentqueue.h"
#include "defs.h"

using namespace std;

class PreProcessingReader {
private:
    class ReadTask {
    public:
        uint64_t blockNumber;
        uint64_t blockSize;

        ReadTask() = default;
        ReadTask(uint64_t blockNumber, uint64_t blockSize) : blockNumber(blockNumber), blockSize(blockSize) {}
    };

    INLINE void separateSingleRecord(
            char*& keyStore,
            char*& readBuffer,
            uint64_t& recordOffset,
            uint64_t& i) {
        char valBuf[4];
        auto no = i + recordOffset;
        memcpy(valBuf, &no, 4);
        copy(readBuffer + i * TUPLE_SIZE,
             readBuffer + i * TUPLE_SIZE + REAL_KEY_SIZE,
             keyStore + (recordOffset + i) * KEY_SIZE);
        copy(valBuf, valBuf + 4,
             keyStore + (recordOffset + i) * KEY_SIZE + REAL_KEY_SIZE);
        i++;
    }

    INLINE void separateKeyValue(
            char*& keyStore,
            char*& valueStore,
            char*& readBuffer,
            uint64_t& readBufferRecordCount,
            uint64_t& recordOffset) {
        //auto separation_start = chrono::high_resolution_clock::now();
        uint64_t remainder = readBufferRecordCount % 4;
        uint64_t i = 0;
        switch (remainder) {
            case 3:
                separateSingleRecord(keyStore, readBuffer, recordOffset, i);
            case 2:
                separateSingleRecord(keyStore, readBuffer, recordOffset, i);
            case 1:
                separateSingleRecord(keyStore, readBuffer, recordOffset, i);
        }
        uint64_t iters = readBufferRecordCount / 4;
        for (size_t iter = 0; iter < iters; iter++) {
            separateSingleRecord(keyStore, readBuffer, recordOffset, i);
            separateSingleRecord(keyStore, readBuffer, recordOffset, i);
            separateSingleRecord(keyStore, readBuffer, recordOffset, i);
            separateSingleRecord(keyStore, readBuffer, recordOffset, i);
        }
        copy(readBuffer,
             readBuffer + readBufferRecordCount * TUPLE_SIZE,
             valueStore + recordOffset * TUPLE_SIZE);
        //auto separation_end = chrono::high_resolution_clock::now();
        //printTime("separation", separation_start, separation_end);
    }

    void readFileBlockAndSeparate(
            char*& keyStore,
            char*& valueStore,
            char* readBuffer,
            moodycamel::ConcurrentQueue<ReadTask>& queue,
            moodycamel::ProducerToken& token) {
        ReadTask task;
        int fd = open(infile.c_str(), O_RDONLY | O_DIRECT, 0666);
        assert(fd != -1);
        while (queue.try_dequeue_from_producer(token, task)) {
            auto blockNumber = task.blockNumber;
            auto blockSize = task.blockSize;
            //cout << "blockNumber " << blockNumber << "blockSize " << blockSize << endl;
            assert(pread(fd, readBuffer, blockSize, blockNumber * kBlockSize) != -1);
            auto countOffset = (blockNumber * kBlockSize) / TUPLE_SIZE;
            auto blockCount = (blockSize / TUPLE_SIZE);
            separateKeyValue(keyStore, valueStore, readBuffer, blockCount, countOffset);
        }
        //assert(close(fd) != -1);
    }

    const uint64_t kBlockSize = 256000;
    string infile;
    uint64_t size;
    uint64_t nThreads;
public:
    PreProcessingReader(string infile, uint64_t nThreads, uint64_t size)
    : infile(std::move(infile)), size(size), nThreads(nThreads) {
    }
    void read(char*& keyStore, char*& valueStore) {
        vector<ReadTask> tasks;
        auto blockNum = (size / kBlockSize);
        auto remainder = size % kBlockSize;

        tasks.reserve(blockNum + 1);
        for (uint64_t i = 0; i < blockNum; i++) {
            tasks.emplace_back(i, kBlockSize);
        }
        if (remainder > 0) {
            tasks.emplace_back(blockNum, remainder);
        }
        ////cout << "all blocks " << tasks.size() << endl;
        moodycamel::ConcurrentQueue<ReadTask> queue(tasks.size());
        moodycamel::ProducerToken token(queue);

        queue.try_enqueue_bulk(token, tasks.data(), tasks.size());
        vector<thread> ioThreads;
        void *readBuffer;
        assert(posix_memalign(&readBuffer, BLOCKSIZE, nThreads*kBlockSize) != -1);
        ioThreads.reserve(nThreads);
        for (uint64_t i = 0; i < nThreads; i++) {
            ioThreads.emplace_back(&PreProcessingReader::readFileBlockAndSeparate, this,
                                   ref(keyStore), ref(valueStore),
                                   static_cast<char*>(readBuffer) + i*kBlockSize,
                                   ref(queue), ref(token));
        }

        for (auto& th : ioThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        //free(readBuffer);
    }
};

#endif //SIGMOD_PREPROCESSING_READER_H
