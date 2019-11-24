#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <memory.h>
#include <algorithm>

typedef long long int64;

using namespace std;

void makeSort(string inputPath, string outputPath, string tmpPath, int64 memoryLimit, int64 blockSize)
{
	ifstream input(inputPath);
	ofstream output(outputPath);

	vector<char> buffer(64 * 1024 * 1024);
	input.read(buffer.data(), buffer.size());
	if (!input.eof()) {
		cerr << "That's too much for my stupid implementation :(" << endl;
        exit(1);
	}

	int byteCount = input.gcount();
    buffer.resize(byteCount);
	int count = byteCount / sizeof(uint64_t);
	vector<uint64_t> numbers(count);
	memcpy(numbers.data(), buffer.data(), count * sizeof(uint64_t));

	sort(numbers.begin(), numbers.end());

	memcpy(buffer.data(), numbers.data(), count * sizeof(uint64_t));
	output.write(buffer.data(), buffer.size());
}

int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Incorrect number of arguments: " << argc - 1 << ", expected 4" << endl;
		cerr << "Usage: <inputPath> <outputPath> <tmpPath> <memoryLimit> <blockSize>" << endl;
        return 1;
    }

	string inputPath = argv[1];
	string outputPath = argv[2];
	string tmpPath = argv[3];
    int64 memoryLimit, blockSize;
    {
        istringstream is(argv[4]);
        is >> memoryLimit;
    }
    {
        istringstream is(argv[5]);
        is >> blockSize;
    }

    makeSort(inputPath, outputPath, tmpPath, memoryLimit, blockSize);

    return 0;
}

