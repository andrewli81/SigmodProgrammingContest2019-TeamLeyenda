#ifndef RADIXSORTER_H
#define RADIXSORTER_H

#include <iostream>
#include <vector>
#include <thread>
#include <algorithm>
#include <string>
#include <cstring>
#include "concurrentqueue.h"
#include "defs.h"

using namespace std;


class RadixSorter {
private:
    uint64_t nThreads;
    uint64_t l2BufferSize;
    uint64_t bufferWidth;
    uint64_t nPartitions;
    uint64_t bigBinThreshold;
    uint64_t interval;
    uint8_t start;
    bool deleteBuffer;

    class Range {
    public:
        uint64_t start;
        uint64_t end;
        uint64_t partition_id;
        Range() = default;
        Range(uint64_t start, uint64_t end, uint64_t partition_id) : start(start), end(end), partition_id(partition_id) {}
    };

    class SmallBinSortTask {
    public:
        uint64_t dataOffset;
        uint64_t nRecs;
        uint64_t radixByte;
        bool isNarrow;
        SmallBinSortTask() = default;
        SmallBinSortTask(uint64_t dataOffset, uint64_t nRecs, uint64_t radixByte, bool isNarrow)
                : dataOffset(dataOffset), nRecs(nRecs), radixByte(radixByte), isNarrow(isNarrow) {}
    };

    //Placeholder class to help calculate the size
    struct Record {
        uint8_t byteArray[KEY_SIZE_RADIX_SORT];
    };

    //Check if radix sort is more profitable
    INLINE bool checkNarrowing(uint64_t x, uint64_t y) { return x < 16 * y; }

    INLINE void tinySort(char*& data, const uint64_t& dataOffset, const uint64_t& radixByte, const uint64_t& nRecs) {
        //cout << "tinySort: " << dataOffset << " radixByte: " << radixByte << " nRecs: " << nRecs << endl;
        void* dataPtr = data + dataOffset;
        auto * recordPtr = static_cast<Record*>(dataPtr);
        Record* recordPtrEnd = recordPtr + nRecs;
        sort(recordPtr, recordPtrEnd, [&](const Record& a, const Record& b) -> bool
        {
            return memcmp(a.byteArray + radixByte, b.byteArray + radixByte, REAL_KEY_SIZE - radixByte) < 0;
        });
    }

    INLINE void buildHistogram(vector<uint64_t>& histogram, const uint64_t& partition_count, char*& data, uint64_t offset)
    {
        // Remainder
        switch (partition_count % 4)
        {
            case 3:
                histogram[charToUint8(data[offset], start)]++;
                offset += KEY_SIZE_RADIX_SORT;
            case 2:
                histogram[charToUint8(data[offset], start)]++;
                offset += KEY_SIZE_RADIX_SORT;
            case 1:
                histogram[charToUint8(data[offset], start)]++;
                offset += KEY_SIZE_RADIX_SORT;
        }

        // Quad Loop
        auto n_iters = partition_count / 4;
        for (int64_t i = 0; i < n_iters; ++i)
        {
            histogram[charToUint8(data[offset], start)]++;
            offset += KEY_SIZE_RADIX_SORT;
            histogram[charToUint8(data[offset], start)]++;
            offset += KEY_SIZE_RADIX_SORT;
            histogram[charToUint8(data[offset], start)]++;
            offset += KEY_SIZE_RADIX_SORT;
            histogram[charToUint8(data[offset], start)]++;
            offset += KEY_SIZE_RADIX_SORT;
        }
    }

    INLINE void simpleScatterRecord(
            char*& data,
            char*& tmp,
            const uint64_t& radixByte,
            uint64_t& radixOffset,
            const uint64_t& dataOffset,
            vector<uint64_t>& globalOffset,
            vector<uint64_t>& scattered,
            const uint64_t& nRecs) {
        auto byteVal = charToUint8(data[radixOffset], start);
        uint64_t recordOffset = radixOffset - radixByte;
        copy(data + recordOffset,
             data + recordOffset + KEY_SIZE_RADIX_SORT,
             tmp + dataOffset + globalOffset[byteVal]
             * KEY_SIZE_RADIX_SORT + scattered[byteVal] * KEY_SIZE_RADIX_SORT);
        scattered[byteVal]++;
        radixOffset += KEY_SIZE_RADIX_SORT;
    }

    INLINE void simpleScatter(
            char*& data,
            char*& tmp,
            const uint64_t radixByte,
            uint64_t& radixOffset,
            const uint64_t& dataOffset,
            vector<uint64_t>& globalOffset,
            const uint64_t& nRecs)
    {
        vector<uint64_t> scattered(interval, 0);
        switch (nRecs % 4)
        {
            case 3:
                simpleScatterRecord(data, tmp, radixByte, radixOffset, dataOffset, globalOffset, scattered, nRecs);
            case 2:
                simpleScatterRecord(data, tmp, radixByte, radixOffset, dataOffset, globalOffset, scattered, nRecs);
            case 1:
                simpleScatterRecord(data, tmp, radixByte, radixOffset, dataOffset, globalOffset, scattered, nRecs);
        }

        for (uint64_t i = nRecs % 4; i < nRecs; i += 4)
        {
            simpleScatterRecord(data, tmp, radixByte, radixOffset, dataOffset, globalOffset, scattered, nRecs);
            simpleScatterRecord(data, tmp, radixByte, radixOffset, dataOffset, globalOffset, scattered, nRecs);
            simpleScatterRecord(data, tmp, radixByte, radixOffset, dataOffset, globalOffset, scattered, nRecs);
            simpleScatterRecord(data, tmp, radixByte, radixOffset, dataOffset, globalOffset, scattered, nRecs);
        }
    }

    INLINE void smallScatterRecord(
            char*& data,
            char*& tmp,
            uint64_t& radixOffset,
            const uint64_t& dataOffset,
            const uint64_t& radixByte,
            vector<uint64_t>& partitionGlobalOffset,
            vector<uint64_t>& scattered,
            char*& l2buffer) {
        uint64_t recordOffset = radixOffset - radixByte;
        auto byteVal = charToUint8(data[radixOffset], start);
        uint64_t index = scattered[byteVal] % bufferWidth;
        copy (  data + recordOffset,
                data + recordOffset + KEY_SIZE_RADIX_SORT,
                l2buffer + (byteVal * bufferWidth + index) * KEY_SIZE_RADIX_SORT);
        scattered[byteVal]++;
        radixOffset += KEY_SIZE_RADIX_SORT;
        if (UNLIKELY(index == (bufferWidth - 1))) {
            auto radixCacheOffset = byteVal * bufferWidth * KEY_SIZE_RADIX_SORT;
            copy (l2buffer + radixCacheOffset,
                  l2buffer + radixCacheOffset + bufferWidth * KEY_SIZE_RADIX_SORT,
                  tmp + dataOffset + partitionGlobalOffset[byteVal] * KEY_SIZE_RADIX_SORT);
            partitionGlobalOffset[byteVal] += bufferWidth;
        }
    }

    INLINE void smallBinRadixSortSubTask(char*& data, char*& tmp,
            moodycamel::ConcurrentQueue<SmallBinSortTask>& queue, char*& buffer, SmallBinSortTask& task,
            const uint64_t& radixByte, bool& mustMoveTmpBackToDataAfterTinySort) {
        auto dataOffset = task.dataOffset;
        auto nRecs = task.nRecs;
        auto isNarrow = task.isNarrow;
        auto radixOffset = dataOffset + radixByte;
        //cout << "Small bin task: dataOffset " << dataOffset << " radixByte " << radixByte << " nRecs " << nRecs << endl;
        if (nRecs <= WIDE_TINY_SORT_THRESHOLD || (isNarrow && nRecs <= TINY_SORT_THRESHOLD))
        {
            if (nRecs > 1) {
                tinySort(data, dataOffset, radixByte, nRecs);
            }
            if ((radixByte % 2) && nRecs) {
                copy(data + dataOffset,
                     data + dataOffset + nRecs * KEY_SIZE_RADIX_SORT,
                     tmp + dataOffset);
            }
            return;
        }

        vector<uint64_t> globalHistogram(interval + 1);
        uint64_t largestBinSize = 0;

        // Build histogram and offset graph
        buildHistogram(globalHistogram, nRecs, data, radixOffset);

        uint64_t prevSum = 0;
        uint64_t tmp_n = 0;
        for (int i = 0; i < interval; ++i)
        {
            if (globalHistogram[i] > largestBinSize) {
                largestBinSize = globalHistogram[i];
            }
            tmp_n = prevSum;
            prevSum += globalHistogram[i];
            globalHistogram[i] = tmp_n;
        }
        globalHistogram[interval] = nRecs;

        //if fits in L2 cache, just do simple scattering without caching first. Otherwise, do cached radix sort
        if (nRecs * KEY_SIZE_RADIX_SORT < 255000) {
            simpleScatter(data, tmp, radixByte, radixOffset, dataOffset, globalHistogram, nRecs);
        } else {
            vector<uint64_t> copyGlobalHistogram = globalHistogram;
            vector<uint64_t> scattered(interval, 0);
            switch (nRecs % 4)
            {
                case 3:
                    smallScatterRecord(data, tmp, radixOffset, dataOffset, radixByte, copyGlobalHistogram, scattered, buffer);
                case 2:
                    smallScatterRecord(data, tmp, radixOffset, dataOffset, radixByte, copyGlobalHistogram, scattered, buffer);
                case 1:
                    smallScatterRecord(data, tmp, radixOffset, dataOffset, radixByte, copyGlobalHistogram, scattered, buffer);
            }

            for (uint64_t i = nRecs % 4; i < nRecs; i += 4)
            {
                smallScatterRecord(data, tmp, radixOffset, dataOffset, radixByte, copyGlobalHistogram, scattered, buffer);
                smallScatterRecord(data, tmp, radixOffset, dataOffset, radixByte, copyGlobalHistogram, scattered, buffer);
                smallScatterRecord(data, tmp, radixOffset, dataOffset, radixByte, copyGlobalHistogram, scattered, buffer);
                smallScatterRecord(data, tmp, radixOffset, dataOffset, radixByte, copyGlobalHistogram, scattered, buffer);
            }

            // Flush remainder of l2 cached records to main memory
            for (int64_t i = 0; i < interval; i++) {
                uint64_t remainder = globalHistogram[i + 1] - copyGlobalHistogram[i];
                if (remainder > 0) {
                    auto radixCacheOffset = i * bufferWidth * KEY_SIZE_RADIX_SORT;
                    copy (buffer + radixCacheOffset,
                          buffer + radixCacheOffset + remainder * KEY_SIZE_RADIX_SORT,
                          tmp + dataOffset + copyGlobalHistogram[i] * KEY_SIZE_RADIX_SORT);
                }
            }
        }

        if (radixByte < REAL_KEY_SIZE - 1)
        {
            //Since tiny sort shortcuts the radix sort sequence, it's possible that the data is still in tmp instead of data.
            if (largestBinSize <= WIDE_TINY_SORT_THRESHOLD || (isNarrow && largestBinSize <= TINY_SORT_THRESHOLD))
            {
                // All bins are tiny - just sort them all.
                for (int i = 0; i < interval; ++i)
                {
                    uint64_t binRec = globalHistogram[i + 1] - globalHistogram[i];
                    if (binRec > 1) {
                        tinySort(tmp, dataOffset + globalHistogram[i] * KEY_SIZE_RADIX_SORT, radixByte + 1, binRec);
                    }
                }
                if (mustMoveTmpBackToDataAfterTinySort) {
                    copy(tmp + dataOffset,
                         tmp + dataOffset + nRecs * KEY_SIZE_RADIX_SORT,
                         data + dataOffset);
                }
            }
            else {
                for (int i = 0; i < interval; ++i) {
                    uint64_t binRec = globalHistogram[i + 1] - globalHistogram[i];

                    if (binRec <= WIDE_TINY_SORT_THRESHOLD || (isNarrow && binRec <= TINY_SORT_THRESHOLD)) {
                        if (binRec > 1) {
                            tinySort(tmp, dataOffset + globalHistogram[i] * KEY_SIZE_RADIX_SORT, (radixByte + 1), binRec);
                        }
                        if (mustMoveTmpBackToDataAfterTinySort && binRec) {
                            copy(tmp + dataOffset + globalHistogram[i] * KEY_SIZE_RADIX_SORT,
                                 tmp + dataOffset + globalHistogram[i + 1] * KEY_SIZE_RADIX_SORT,
                                 data + dataOffset + globalHistogram[i] * KEY_SIZE_RADIX_SORT);
                        }
                    } else {
                        queue.enqueue(
                                SmallBinSortTask(
                                        dataOffset + globalHistogram[i] * KEY_SIZE_RADIX_SORT,
                                        binRec,
                                        (radixByte + 1),
                                        checkNarrowing(nRecs, binRec)));
                    }
                }
            }
        }
    }

    //Small bin radix sort do no spawn threads to parallelize the scatter process. Instead it processes them in a single queue
    void smallBinRadixSort(
            char*& data,
            char*& tmp,
            moodycamel::ConcurrentQueue<SmallBinSortTask>& queue)
    {
        SmallBinSortTask task;
        char* buffer = new char[l2BufferSize];
        while (queue.try_dequeue(task)) {
            auto radixByte = task.radixByte;
            bool mustMoveTmpBackToDataAfterTinySort = !static_cast<bool>(radixByte % 2);
            //Decide to scatter from (data to tmp) or (tmp to data)
            if (mustMoveTmpBackToDataAfterTinySort) {
                smallBinRadixSortSubTask(data, tmp, queue, buffer, task, radixByte, mustMoveTmpBackToDataAfterTinySort);
            } else {
                smallBinRadixSortSubTask(tmp, data, queue, buffer, task, radixByte, mustMoveTmpBackToDataAfterTinySort);
            }
        }
        if (deleteBuffer) {
            delete[] buffer;
        }
    }

    void buildHistogramForPartition(
            char*& data,
            vector<vector<uint64_t>>& globalHistogram,
            const uint64_t radixByte,
            const uint64_t dataOffset,
            moodycamel::ConcurrentQueue<Range>& queue,
            const moodycamel::ProducerToken& mainProducerToken
    )
    {
        Range range;
        while (queue.try_dequeue_from_producer(mainProducerToken, range)) {
            vector<uint64_t> partitionHisto(interval, 0);
            uint64_t partitionCount = range.end - range.start;
            uint64_t offset = dataOffset + range.start * KEY_SIZE_RADIX_SORT + radixByte;
            buildHistogram(partitionHisto, partitionCount, data, offset);
            const auto& thisID = range.partition_id;
            globalHistogram[thisID] = partitionHisto;
        }
    }

    INLINE vector<Range> buildRanges(const uint64_t& nRecs, const uint64_t& nParts) {
        uint64_t partition_size = nRecs / nParts;
        uint64_t start = 0;
        uint64_t end = 0;
        vector<Range> ranges;
        ranges.reserve(nParts);
        for (int64_t i = 0; i < nParts - 1; i++) {
            end += partition_size;
            //cout << "start: " << start << " end: " << end << " partition id: " << i << endl;
            ranges.emplace_back(start, end, i);
            start = end;
        }
        //cout << "start: " << start << " end: " << nRecs << " partition id: " << nParts - 1 << endl;
        ranges.emplace_back(start, nRecs, nParts - 1);
        return ranges;
    }

    INLINE void scatterRecord(
            char*& data,
            char*& tmp,
            uint64_t& radixOffset,
            const uint64_t& dataOffset,
            const uint64_t radixByte,
            vector<uint64_t>& partitionGlobalOffset,
            vector<uint64_t>& recordsToScatter,
            vector<uint64_t>& scattered,
            char*& l2buffer) {
        auto byteVal = charToUint8(data[radixOffset], start);

        uint64_t recordOffset = radixOffset - radixByte;
        uint64_t index = scattered[byteVal] % bufferWidth;
        copy (data + recordOffset,
                data + recordOffset + KEY_SIZE_RADIX_SORT,
                l2buffer + (byteVal * bufferWidth + index) * KEY_SIZE_RADIX_SORT);
        scattered[byteVal]++;
        radixOffset += KEY_SIZE_RADIX_SORT;
        if (UNLIKELY(index == (bufferWidth - 1))) {
            auto radixCacheOffset = byteVal * bufferWidth * KEY_SIZE_RADIX_SORT;
            copy (l2buffer + radixCacheOffset,
                  l2buffer + radixCacheOffset + bufferWidth * KEY_SIZE_RADIX_SORT,
                  tmp + dataOffset + partitionGlobalOffset[byteVal] * KEY_SIZE_RADIX_SORT);
            recordsToScatter[byteVal] -= bufferWidth;
            partitionGlobalOffset[byteVal] += bufferWidth;
        }
    }

    void scatterRecordsToMainMemory(
            char*& data,
            char*& tmp,
            const uint64_t radixByte,
            const uint64_t& dataOffset,
            vector<vector<uint64_t>>& globalOffset,
            vector<vector<uint64_t>>& histograms,
            moodycamel::ConcurrentQueue<Range>& queue,
            const moodycamel::ProducerToken& mainProducerToken)
    {
        Range range;
        char* buffer = new char[l2BufferSize];
        while (queue.try_dequeue_from_producer(mainProducerToken, range))
        {
            uint64_t partitionCount = range.end - range.start;
            uint64_t radixOffset = dataOffset + range.start * KEY_SIZE_RADIX_SORT + radixByte;

            auto& partitionGlobalOffset = globalOffset[range.partition_id];
            auto& recordsToScatter = histograms[range.partition_id];
            vector<uint64_t> scattered(interval, 0);

            switch (partitionCount % 4)
            {
                case 3:
                    scatterRecord(data, tmp, radixOffset, dataOffset, radixByte, partitionGlobalOffset, recordsToScatter, scattered, buffer);
                case 2:
                    scatterRecord(data, tmp, radixOffset, dataOffset, radixByte, partitionGlobalOffset, recordsToScatter, scattered, buffer);
                case 1:
                    scatterRecord(data, tmp, radixOffset, dataOffset, radixByte, partitionGlobalOffset, recordsToScatter, scattered, buffer);
            }

            for (uint64_t i = partitionCount % 4; i < partitionCount; i += 4)
            {
                scatterRecord(data, tmp, radixOffset, dataOffset, radixByte, partitionGlobalOffset, recordsToScatter, scattered, buffer);
                scatterRecord(data, tmp, radixOffset, dataOffset, radixByte, partitionGlobalOffset, recordsToScatter, scattered, buffer);
                scatterRecord(data, tmp, radixOffset, dataOffset, radixByte, partitionGlobalOffset, recordsToScatter, scattered, buffer);
                scatterRecord(data, tmp, radixOffset, dataOffset, radixByte, partitionGlobalOffset, recordsToScatter, scattered, buffer);
            }

            // Flush remainder of l2 cached records to main memory
            for (int64_t i = 0; i < interval; i++) {
                if (recordsToScatter[i] > 0) {
                    auto radixCacheOffset = i * bufferWidth * KEY_SIZE_RADIX_SORT;
                    copy (buffer + radixCacheOffset,
                          buffer + radixCacheOffset + recordsToScatter[i] * KEY_SIZE_RADIX_SORT,
                          tmp + dataOffset + partitionGlobalOffset[i] * KEY_SIZE_RADIX_SORT);
                }
            }
        }
        if (deleteBuffer) {
            delete[] buffer;
        }
    }

    void bigBinRadixSort(
            char*& data,
            char*& tmp,
            const uint64_t& dataOffset,
            const uint64_t& nRecs,
            const uint64_t& radixByte) {
        //cout << "bigBinRadixSort: nRecs " << nRecs << " dataOffset " << dataOffset << " radixByte " << radixByte << endl;
        if (nRecs <= TINY_SORT_THRESHOLD)
        {
            if (nRecs > 1) {
                tinySort(data, dataOffset, radixByte, nRecs);
            }
            if ((radixByte % 2) && nRecs) {
                copy(data + dataOffset,
                     data + dataOffset + nRecs * KEY_SIZE_RADIX_SORT,
                     tmp + dataOffset);
            }
            return;
        }
        //stage 1 - Calculate histogram for each thread
        vector<thread> threads;
        vector<vector<uint64_t>> histograms(nPartitions);
        vector<vector<uint64_t>> globalOffset(nPartitions, vector<uint64_t>(interval, 0));
        auto ranges = buildRanges(nRecs, nPartitions);
        moodycamel::ConcurrentQueue<Range> rangeQueue(nPartitions);
        moodycamel::ProducerToken mainProducerToken(rangeQueue);

        rangeQueue.enqueue_bulk(mainProducerToken, ranges.data(), nPartitions);

        for (uint64_t th_id = 0; th_id < nThreads; ++th_id) {
            threads.emplace_back(&RadixSorter::buildHistogramForPartition, this,
                                 ref(data), ref(histograms), dataOffset, radixByte, ref(rangeQueue), ref(mainProducerToken));
        }

        for (auto &th : threads) {
            if (th.joinable()) {
                th.join();
            }
        }
        threads.clear();
//        for (int i = 0; i < nPartitions; i++) {
//            for (int j = 0; j < interval; j++) {
//                cout << histograms[i][j] << " ";
//            }
//            cout << endl;
//        }

        // Calculate global offset of each partition
        for (int64_t k = 0; k < nPartitions - 1; k++) {
            globalOffset[nPartitions - 1][0] += histograms[k][0];
        }

        for (int64_t m = 1; m < interval; m++) {
            globalOffset[nPartitions - 1][m] += histograms[nPartitions - 1][m - 1];
            globalOffset[nPartitions - 1][m] += globalOffset[nPartitions - 1][m - 1];
            for (int64_t n = 0; n < nPartitions - 1; n++) {
                globalOffset[nPartitions - 1][m] += histograms[n][m];
            }
        }

        if (nPartitions >= 2) {
            for (int64_t i = nPartitions - 2; i >= 0; i--) {
                for (int64_t j = 0; j < interval; j++) {
                    globalOffset[i][j] = globalOffset[i + 1][j] - histograms[i][j];
                }
            }
        }

//        for (int i = 0; i < nPartitions; i++) {
//            for (int j = 0; j < interval; j++) {
//                cout << globalOffset[i][j];
//            }
//            cout << endl;
//        }

        vector<uint64_t> finalRadixOffset(interval);
        for (int64_t i = 0; i < interval - 1; ++i) {
            auto n = globalOffset[0][i + 1] - globalOffset[0][i];
            finalRadixOffset[i] = n;
        }
        finalRadixOffset[interval - 1] = nRecs - globalOffset[0][interval - 1];

        //stage 2 - Scatter partitions to correct locations in tmp

        rangeQueue.enqueue_bulk(mainProducerToken, ranges.data(), nPartitions);

        for (int64_t th_id = 0; th_id < nThreads; ++th_id) {
            threads.emplace_back(&RadixSorter::scatterRecordsToMainMemory, this,
                                 ref(data), ref(tmp), radixByte, dataOffset, ref(globalOffset), ref(histograms),
                                 ref(rangeQueue), ref(mainProducerToken));
        }

        for (auto& th : threads) {
            if (th.joinable()) {
                th.join();
            }
        }

        threads.clear();

        //Sort big bins using multi-threaded radix sort, and small bins using single-threaded radix sort. Small bins are
        //placed into queues and threads would consume the small bin radix sort tasks after all big bin tasks are processed
        //in current level.
        //This process is recursively done using all threads.
        if (radixByte < REAL_KEY_SIZE - 1)
        {
            uint64_t _dataOffset = dataOffset;

            moodycamel::ConcurrentQueue<SmallBinSortTask> smallBinTaskQueue(interval);

            for (int64_t i = 0; i < interval; ++i)
            {
                auto n = finalRadixOffset[i];
                //cout << " n " << n << endl;
                if (n > bigBinThreshold)
                {
                    bigBinRadixSort(tmp, data, _dataOffset, n, radixByte + 1);
                } else if (n > 0) {
                    smallBinTaskQueue.enqueue(SmallBinSortTask(_dataOffset, n, radixByte + 1, checkNarrowing(nRecs, n)));
                }
                _dataOffset += n * KEY_SIZE_RADIX_SORT;
            }

            // Make sure that the smallBinRadixSort always have data->data, tmp->tmp relationship
            if (radixByte % 2) {
                for (int64_t i = 0; i < nThreads; i++) {
                    threads.emplace_back(&RadixSorter::smallBinRadixSort, this, ref(tmp), ref(data),
                                         ref(smallBinTaskQueue));
                }
            } else {
                for (int64_t i = 0; i < nThreads; i++) {
                    threads.emplace_back(&RadixSorter::smallBinRadixSort, this, ref(data), ref(tmp),
                                         ref(smallBinTaskQueue));
                }
            }

            for (auto& th : threads) {
                if (th.joinable()) {
                    th.join();
                }
            }
        }
    }
public:
    explicit RadixSorter(const uint64_t nThreads, bool ifDeleteBuffer, const uint8_t _start, const uint64_t _interval
    ): nThreads(nThreads), start(_start), interval(_interval)
    {
        nPartitions = nThreads * FIRST_PASS_THREAD_MULTIPLIER;
        bufferWidth = (((L2_CACHE_SIZE / interval) - 24) / KEY_SIZE_RADIX_SORT);
        uint64_t tryWidth = bufferWidth;
        for (uint64_t i = tryWidth; tryWidth > 0; tryWidth--) {
            if ((i * KEY_SIZE_RADIX_SORT) % 64 == 0) {
                tryWidth = i;
                break;
            }
        }
        if (tryWidth != 0) {
            bufferWidth = tryWidth;
        }
        l2BufferSize = interval * bufferWidth * KEY_SIZE_RADIX_SORT;
        bigBinThreshold = 0;
        deleteBuffer = ifDeleteBuffer;
    }

    void startSort(char*& data, char*& tmp, const uint64_t nRecs) {
        bigBinThreshold = 2 * nRecs / (3 * nThreads);
        //bigBinThreshold = 10000;
        //cout << "bigBinThreshold: " << bigBinThreshold << endl;
        bigBinRadixSort(data, tmp, 0, nRecs, 0);
    }
};
#endif //RADIXSORTER_H