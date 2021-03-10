#include <iostream>
#include <vector>
#include <thread>
#include <fstream>
#include <algorithm>
#include <string>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include "sort_base.h"
#include "sort_60.h"
#include "sort_20.h"
#include "sort_20_new.h"
#include "sort_10.h"
using namespace std;

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "USAGE: " << argv[0] << " [in-file] [outfile]\n";
        exit(-1);
    }
    cout << argv[1] << endl;
    cout << argv[2] << endl;
    ifstream is(argv[1]);
    if (!is) {
        cerr << "Could not open the file\n";
        exit(-1);
    }
    // get size of file
    is.seekg(0, ifstream::end);
    const int64_t size = is.tellg();
    cout << size << endl;
    is.close();
    if (size < 0) {
        return 0;
    }else if(size < 15000000000){
        // sort 10GB file
        sort_10(argv[1], argv[2], size);
    }else if(size < 25000000000){
        // sort 20GB file
        sort_20_new(argv[1], argv[2], size);
    }else{
        // sort 60GB file
        sort_60(argv[1], argv[2], size);
        return 0;
    }
    return 0;
}
