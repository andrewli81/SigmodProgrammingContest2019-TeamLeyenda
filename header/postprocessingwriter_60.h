//
// Created by andrew on 4/3/19.
//

#ifndef JVM_OVERFLOW_POSTPROCESSINGWRITER60_H
#define JVM_OVERFLOW_POSTPROCESSINGWRITER60_H

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
#include "trunkreader.h"
#include "defs.h"
using namespace std;

class PostProcessingWriter60 {
private:

    INLINE void readFromFile(
            char*& buffer,
            const uint64_t bufferNo,
            const uint64_t readSize,
            const uint64_t bufferSize,
            array<atomic<bool>, 28>& locks) {
        string fileName = TEMP_FILE_NAME + to_string(bufferNo);
        //cout << "read total size " << readSize / readBufferSize << endl;
        for(int i = 0; i < (readSize / readBufferSize); i++) {
            //cout << "start read wait " << i << " bufferNo " << bufferNo << endl;
            while(locks[i]){
            }
            //auto read_start = chrono::high_resolution_clock::now();
            auto overwriteOffset = (i % (bufferSize / readBufferSize)) * readBufferSize;
            //cout << "read " << fileName << " from offset " << i * readBufferSize << endl;
            TrunkReader(fileName, THREADS, i * readBufferSize, readBufferSize).read(buffer + overwriteOffset);
            //auto read_end = chrono::high_resolution_clock::now();
            //printTime("Read from file", read_start, read_end);
            locks[i + 14] = true;
        }
    }

    string infile;
    string outfile;
    uint64_t numThreads;
    uint64_t size;
    uint64_t trunkSize;
    uint64_t partitionSize = 100000000;
    uint64_t readBufferSize = 250000000;
public:
    PostProcessingWriter60(
            const string infile,
            const string outfile,
            uint64_t numThreads,
            uint64_t size,
            uint64_t trunkSize) :
            infile(infile),
            outfile(outfile),
            numThreads(numThreads),
            size(size),
            trunkSize(trunkSize){
    }

    /*
     * For dataBuffer 1-10,
     *
     *
     * 1.5 GB each
     * For dataBuffer 11-12, 5 GB
     */
    INLINE void write(
            uint64_t trunks,
            uint64_t trunkSize,
            uint64_t inMemoryTrunkSize,
            char*& dataBuffer,
            char*& data) {
        //auto write_file_start = chrono::high_resolution_clock::now();
        //cout << "trunks " << endl;
        //cout << "trunksize " << trunkSize << endl;
        //cout << "in memory trunksize " << inMemoryTrunkSize << endl;

        vector<char*> dataBuffers(trunks);
        vector<array<atomic<bool>, 28>> locks(trunks - 1);
        vector<thread> readers(trunks - 1);
        vector<uint64_t> offsets(trunks, 100);

        for (uint64_t i = 0; i < trunks - 1; i++) {
            dataBuffers[i] = dataBuffer + i * inMemoryTrunkSize;
            for(int j = 0; j < 28; j++) {
                locks[i][j].store(j < 14);
            }
            readers[i] = thread(&PostProcessingWriter60::readFromFile, this, ref(dataBuffers[i]), i,
                    (trunkSize - inMemoryTrunkSize), inMemoryTrunkSize, ref(locks[i]));
        }
        dataBuffers[trunks - 1] = data;

        char* outputBuffer1st = new char[partitionSize];
        char* outputBuffer2nd = new char[partitionSize];

        uint64_t partitionNum = size / partitionSize;
        uint64_t partitionTupleNum = partitionSize / TUPLE_SIZE;

        moodycamel::BlockingConcurrentQueue<char*> writePartition(partitionNum);

        ofstream of(outfile);
        atomic<bool> dataReady(true);

        thread writer([&]() {
            // Affine Consumer thread to CPU core 1
            setThreadAffinity(1);
            for (int i = 0; i < partitionNum; ++i) {
                char* item;
                writePartition.wait_dequeue(item);
                of.write(item, partitionSize);
                dataReady = false;

            }
            of.close();
        });

        priority_queue<Rec> q;

        uint64_t n = 0;

        for(int i = 0; i < trunks; i++) {
            auto tmp = new char[TUPLE_SIZE];
            memcpy(tmp, dataBuffers[i], TUPLE_SIZE);
            q.emplace(Rec{tmp, i});
        }

        for(size_t partition = 0 ; partition < partitionNum; partition++) {
            auto outputBuffer = (partition % 2 == 0 ? outputBuffer1st : outputBuffer2nd);
            for (auto i = 0; i < partitionSize; i += 100) {
                auto tmpRec = q.top();
                auto tmp = tmpRec.binaryRec;
                auto whichBuffer = tmpRec.whichBuffer;
                copy(tmp, tmp + TUPLE_SIZE, outputBuffer + i);
                q.pop();
                if (LIKELY(offsets[whichBuffer] < trunkSize)) {
                    if (whichBuffer < trunks - 1) {
                        memcpy(tmp, dataBuffers[whichBuffer] + offsets[whichBuffer] % inMemoryTrunkSize,
                               TUPLE_SIZE);
                        q.emplace(Rec{tmp, whichBuffer});
                        offsets[whichBuffer] += 100;
                        auto offset = offsets[whichBuffer];
                        if (UNLIKELY(offset % readBufferSize == 0)) {
                            //cout << "write finished " << offset << " buffer " << whichBuffer << endl;
                            auto curReadBuffer = offset / readBufferSize  - 1;

                            // while offset <= 3.5g, 0 - 6 -> 7 - 13
                            if(offset <= (trunkSize - inMemoryTrunkSize))
                                locks[whichBuffer][curReadBuffer] = false;
                            // while offset >= 1.5 but less than 5GB, check locks 7 - 13
                            if(inMemoryTrunkSize <= offset && offset < trunkSize) {
                                //cout << whichBuffer << "'s offset: " << offset << "position:" << curReadBuffer << endl;
                                while (!locks[whichBuffer][curReadBuffer + 9]){
                                }
                            }
                        }
                    } else {
                        q.emplace(Rec{dataBuffers[whichBuffer] + offsets[whichBuffer], whichBuffer});
                        offsets[whichBuffer] += 100;
                    }
                }
            }
            writePartition.enqueue(outputBuffer);

            if (UNLIKELY(partition == 0 || partition == partitionNum - 1)) {
                continue;
            } else {
                while (dataReady.load()) {
                }
                dataReady = true;
            }

        }
        for (auto& reader : readers) {
            if (reader.joinable()) {
                reader.join();
            }
        }
        if (writer.joinable()) {
            writer.join();
        }

        //auto write_file_end = chrono::high_resolution_clock::now();
        //printTime("merge files: ", write_file_start, write_file_end);
        assert(q.empty());
    }
};


#endif //JVM_OVERFLOW_POSTPROCESSINGWRITER60_H
