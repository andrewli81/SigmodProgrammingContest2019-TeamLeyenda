#ifdef _MSC_VER 
#include <ppl.h>
#elif __GNUG__
#include <omp.h>
#include <parallel/algorithm>
#endif
#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <thread> 
#include <string>
#include "header/defs.h"

using namespace std;
const int kTupleSize = 100;
const int kKeySize = 10;
const int64_t kNumThreads = 4;
int kFileOffset;

inline void readFileBase(
        vector<char> &buffer,
        const int64_t fdOffset,
        const string &infile){
    ifstream is(infile);
    is.seekg(fdOffset);
    char* c = buffer.data();
    char* cTd = c + fdOffset;
    is.read(cTd, kFileOffset);
    is.close();
}

inline void writeFileBase(
        vector<char> &buffer,
        const int64_t fdOffset,
        const string &outfile){
    ofstream os(outfile);
    os.seekp(fdOffset);
    char* c = buffer.data();
    char* cTd = c + fdOffset;
    os.write(cTd, kFileOffset);
    os.close();
}

inline vector<array<char,100>> baseSort(
        const string &infile,
        const string &outfile,
        bool ifWrite) {
  ifstream is(infile);
  if (!is) {
    cerr << "Could not open the file\n";
    exit(-1);
  }

  // get size of file
  is.seekg(0, is.end);
  const int64_t size = is.tellg();
  is.seekg(0);

  // allocate memory for file content
  const int64_t num_tuples = size / kTupleSize;
  vector<array<char, kTupleSize>> buffer(num_tuples);
  kFileOffset = size/kNumThreads;

  //----------------READ FR FILE PART-------------------// 
  //----------------READ FR FILE PART-------------------// 
  //----------------READ FR FILE PART-------------------// 
  //----------------READ FR FILE PART-------------------// 
  //----------------READ FR FILE PART-------------------// 
  //----------------READ FR FILE PART-------------------// 

  auto read_file_start = chrono::high_resolution_clock::now();
  for (std::int64_t i = 0; i < num_tuples; ++i) {
    is.read(buffer[i].data(), kTupleSize);
  }
  auto read_file_stop = chrono::high_resolution_clock::now();
  printTime("read file", read_file_start, read_file_stop);
  is.close();

  //---------------------SORT PART-----------------------//
  //---------------------SORT PART-----------------------//
  //---------------------SORT PART-----------------------//
  //---------------------SORT PART-----------------------//
  //---------------------SORT PART-----------------------//
  //---------------------SORT PART-----------------------//
  auto sort_start = chrono::high_resolution_clock::now(); 
  //kx::radix_sort(buffer.begin(), buffer.end(), RadixTraitsArray());
  /*#ifdef _MSC_VER
    std::cout << "Here there"
		concurrency::parallel_sort(res_val_as_T, res_val_as_T + params.n_recs.val);		
  #elif __GNUG__
		omp_set_num_threads(40);
		__gnu_parallel::sort(buffer.begin(), buffer.end(),
            [](const array<char, kTupleSize> &lhs,
               const array<char, kTupleSize> &rhs) {
              for (int i = 0; i < kKeySize; ++i) {
                if (lhs[i] == rhs[i]) {
                  continue;
                }
                return lhs[i] < rhs[i];
              }
              return false;
            });;
  #else
		sort(res_val_as_T, res_val_as_T + params.n_recs.val);
  #endif*/
  //omp_set_num_threads(2);
  __gnu_parallel::sort(buffer.begin(), buffer.end(),
            [](const array<char, kTupleSize> &lhs,
               const array<char, kTupleSize> &rhs) {
              for (int i = 0; i < kKeySize; ++i) {
                if (lhs[i] == rhs[i]) {
                  continue;
                }
                return unsigned(lhs[i]) < unsigned(rhs[i]);
              }
              return false;
            });
  auto sort_stop = chrono::high_resolution_clock::now();
  printTime("sort", sort_start, sort_stop);

  //----------------WRITE TO FILE PART-------------------// 
  //----------------WRITE TO FILE PART-------------------// 
  //----------------WRITE TO FILE PART-------------------// 
  //----------------WRITE TO FILE PART-------------------// 
  //----------------WRITE TO FILE PART-------------------// 
  //----------------WRITE TO FILE PART-------------------// 
  if(ifWrite) {
      auto write_file_start = chrono::high_resolution_clock::now();
      ofstream os(outfile);
      for (int64_t i = 0; i < num_tuples; ++i) {
          os.write(buffer[i].data(), kTupleSize);
      }
      auto write_file_stop = chrono::high_resolution_clock::now();
      printTime("write file", write_file_start, write_file_stop);
      os.close();
  }
  return buffer;
}

inline vector<array<char,100>> baseSortParallel(
        const string &infile,
        const string &outfile) {
    ifstream is(infile);
    if (!is) {
        cerr << "Could not open the file\n";
        exit(-1);
    }

    // get size of file
    is.seekg(0, is.end);
    const int64_t size = is.tellg();
    is.seekg(0);

    // allocate memory for file content
    const int64_t num_tuples = size / kTupleSize;
    vector<array<char, kTupleSize>> buffer(num_tuples);
    vector<thread> threads;
    kFileOffset = size/kNumThreads;

    //----------------READ FR FILE PART-------------------//
    //----------------READ FR FILE PART-------------------//
    //----------------READ FR FILE PART-------------------//
    //----------------READ FR FILE PART-------------------//
    //----------------READ FR FILE PART-------------------//
    //----------------READ FR FILE PART-------------------//

    auto read_file_start = chrono::high_resolution_clock::now();
    for (int64_t noThread = 0; noThread < kNumThreads; ++noThread) {
      int64_t fdOffset = 0 + noThread * kFileOffset;
      //threads.push_back(thread(readFileBase, ref(buffer), fdOffset, ref(infile)));
    }
    for (auto &td : threads)
      td.join();
    threads.clear();

    auto read_file_stop = chrono::high_resolution_clock::now();
    printTime("read file", read_file_start, read_file_stop);
    is.close();

    //---------------------SORT PART-----------------------//
    //---------------------SORT PART-----------------------//
    //---------------------SORT PART-----------------------//
    //---------------------SORT PART-----------------------//
    //---------------------SORT PART-----------------------//
    //---------------------SORT PART-----------------------//
    auto sort_start = chrono::high_resolution_clock::now();
    __gnu_parallel::stable_sort(buffer.begin(), buffer.end(),
                                [](const array<char, kTupleSize> &lhs,
                                   const array<char, kTupleSize> &rhs) {
                                    for (int i = 0; i < kKeySize; ++i) {
                                        if (lhs[i] == rhs[i]) {
                                            continue;
                                        }
                                        return unsigned(lhs[i]) < unsigned(rhs[i]);
                                    }
                                    return false;
                                });
    auto sort_stop = chrono::high_resolution_clock::now();
    printTime("sort", sort_start, sort_stop);

    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//
    //----------------WRITE TO FILE PART-------------------//

    auto write_file_start = chrono::high_resolution_clock::now();
    ofstream os(outfile);
    for (int64_t noThread = 0; noThread < kNumThreads; ++noThread) {
      const int64_t fdOffset = 0 + noThread * kFileOffset;
      //threads.push_back(thread(writeFileBase, ref(buffer), fdOffset, ref(outfile)));
    }
    for (auto &td : threads)
        td.join();
    threads.clear();
    auto write_file_stop = chrono::high_resolution_clock::now();
    printTime("write file", write_file_start, write_file_stop);
    os.close();
    return buffer;
}

/*int main(int argc, char *argv[]) {
  if (argc != 3) {
    cout << "USAGE: " << argv[0] << " [in-file] [outfile]\n";
    exit(-1);
  }

  sigmodSort(argv[1], argv[2]);
  return 0;
}*/

