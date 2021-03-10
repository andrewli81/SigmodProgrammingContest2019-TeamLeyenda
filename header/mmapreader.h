//
// Created by andrew on 4/29/19.
//

#ifndef JVM_OVERFLOW_MMAPREADER_H
#define JVM_OVERFLOW_MMAPREADER_H
#include <string>
#include <vector>
#include <fstream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "defs.h"

using namespace std;

class MMapReader {
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
            char*& valueStore,
            uint64_t& recordOffset,
            uint64_t& i) {
        char valBuf[4];
        auto no = i + recordOffset;
        memcpy(valBuf, &no, 4);
        copy(valueStore + no * TUPLE_SIZE,
             valueStore + no * TUPLE_SIZE + REAL_KEY_SIZE,
             keyStore + no * KEY_SIZE);
        copy(valBuf, valBuf + 4,
             keyStore + no * KEY_SIZE + REAL_KEY_SIZE);
        i++;
    }

    INLINE void separateKeyValue(
            char*& keyStore,
            char*& valueStore,
            uint64_t& readBufferRecordCount,
            uint64_t& recordOffset) {
        //auto separation_start = chrono::high_resolution_clock::now();
        uint64_t remainder = readBufferRecordCount % 4;
        uint64_t i = 0;
        switch (remainder) {
            case 3:
                separateSingleRecord(keyStore, valueStore, recordOffset, i);
            case 2:
                separateSingleRecord(keyStore, valueStore, recordOffset, i);
            case 1:
                separateSingleRecord(keyStore, valueStore, recordOffset, i);
        }
        uint64_t iters = readBufferRecordCount / 4;
        for (size_t iter = 0; iter < iters; iter++) {
            separateSingleRecord(keyStore, valueStore, recordOffset, i);
            separateSingleRecord(keyStore, valueStore, recordOffset, i);
            separateSingleRecord(keyStore, valueStore, recordOffset, i);
            separateSingleRecord(keyStore, valueStore, recordOffset, i);
        }
        //auto separation_end = chrono::high_resolution_clock::now();
        //printTime("separation", separation_start, separation_end);
    }

    void readFileBlockAndSeparate(
            char*& keyStore,
            char*& valueStore,
            moodycamel::ConcurrentQueue<ReadTask>& queue,
            moodycamel::ProducerToken& token) {
        ReadTask task;

        while (queue.try_dequeue_from_producer(token, task)) {
            auto blockNumber = task.blockNumber;
            auto blockSize = task.blockSize;
            //cout << "blockNumber " << blockNumber << "blockSize " << blockSize << endl;
            auto countOffset = (blockNumber * kBlockSize) / TUPLE_SIZE;
            auto blockCount = (blockSize / TUPLE_SIZE);
            separateKeyValue(keyStore, valueStore, blockCount, countOffset);
        }
        //assert(close(fd) != -1);
    }

    const uint64_t kBlockSize = 256000000;
    string infile;
    uint64_t size;
    uint64_t nThreads;
public:
    MMapReader(string infile, uint64_t nThreads, uint64_t size)
            : infile(std::move(infile)), size(size), nThreads(nThreads) {
    }
    char* read(char*& keyStore) {
        int fd = open(infile.c_str(), O_RDONLY, 0666);
        assert(fd != -1);

        auto writeFile = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        assert(writeFile != (caddr_t) -1);

        char* valueStore = static_cast<char*>(writeFile);

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

        ioThreads.reserve(nThreads);
        for (uint64_t i = 0; i < nThreads; i++) {
            ioThreads.emplace_back(&MMapReader::readFileBlockAndSeparate, this,
                                   ref(keyStore), ref(valueStore), ref(queue), ref(token));
        }

        for (auto& th : ioThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        return valueStore;
        //free(readBuffer);
    }
};

#endif //JVM_OVERFLOW_MMAPREADER_H
