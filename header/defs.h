//
// Created by Xueqing Li on 2019-04-05.
//

#ifndef SIGMOD_DEFS_H
#define SIGMOD_DEFS_H
#define TINY_SORT_THRESHOLD 384
#define WIDE_TINY_SORT_THRESHOLD 32
#define FIRST_PASS_THREAD_MULTIPLIER 8
#define UNLIKELY(condition) __builtin_expect(static_cast<bool>(condition), 0)
#define LIKELY(condition) __builtin_expect(static_cast<bool>(condition), 1)
#define L2_CACHE_SIZE 256000
#define INLINE inline __attribute__((always_inline))
#define TUPLE_SIZE 100
#define KEY_SIZE_RADIX_SORT 14
#define REAL_KEY_SIZE 10
#define KEY_SIZE 14
#define THREADS 40
#define BLOCKSIZE 512
#define TEMP_FILE_NAME "/output-disk/temp_file"

#include <assert.h>
using namespace std;

//Convert signed char (-128 to 127) into 0-255 int32, only works for x86_64 platforms where chars are signed
INLINE uint8_t charToUint8(const char &c, const uint8_t& offset) {
    return static_cast<uint8_t>(c) - offset;
}

INLINE void printTime(
        string&& x,
        chrono::high_resolution_clock::time_point &start,
        chrono::high_resolution_clock::time_point &stop){
    auto duration = chrono::duration_cast<chrono::microseconds>(stop - start);
    std::cout << x << " time: "
         << duration.count() << " microseconds" << std::endl;
}

INLINE void setThreadAffinity(int threadId) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(threadId, &cpuset);

    pthread_t current_thread = pthread_self();
    int rc = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        std::cout << "Set thread affinity error: " << rc << ", " << strerror(errno) << std::endl;
    }
}

class Data {
public:
    char* keyData;
    char* trunkData;
    Data(char* key, char* data) {
        keyData = key;
        trunkData = data;
    }
    Data() {}
};

class Rec {
public:
    char* binaryRec;
    int whichBuffer;

    INLINE bool operator<(const Rec& rhs) const{
        for (int i = 0; i < REAL_KEY_SIZE; ++i) {
            if (binaryRec[i] == rhs.binaryRec[i]) {
                continue;
            }
            return unsigned(rhs.binaryRec[i]) < unsigned(binaryRec[i]);
        }
        return true;
    }
};

#endif //SIGMOD_DEFS_H