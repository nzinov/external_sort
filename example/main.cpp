#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>
#include <memory.h>
#include <algorithm>

using namespace std;

struct TBlockPointer {
    TBlockPointer(size_t partIdx, size_t pointer, size_t value)
        : PartIdx(partIdx)
        , Pointer(pointer)
        , Value(value) {}

    bool operator<(const TBlockPointer& other) {
        return Value < other.Value;
    }

    size_t PartIdx;
    size_t Pointer;
    size_t Value;
};

struct TBlockReader {
    TBlockReader(ifstream& reader, size_t dataSize, size_t blockSize, size_t startOffset)
        : Reader(reader)
        , DataSize(dataSize)
        , BlockSize(blockSize)
        , StartOffset(startOffset) {
        LoadBlock();
    }

    void LoadBlock() {
        size_t offset = StartOffset + BlockIdx * BlockSize;
        size_t size = DataSize - offset;
        if (size > BlockSize) {
            size = BlockSize;
        } else {
            Eof = true;
        }
        Reader.seekg(offset * sizeof(uint64_t));
        Block.resize(size);
        Reader.read(reinterpret_cast<char*>(Block.data()), size * sizeof(uint64_t));
        ++BlockIdx;
    }

    ifstream& Reader;
    size_t DataSize;
    size_t BlockSize;
    size_t StartOffset;
    size_t BlockIdx = 0;
    bool Eof = false;
    vector<uint64_t> Block;
};

void Sort(string inputPath, string outputPath, string tmpPath, size_t memoryLimit, size_t blockSize)
{
    const size_t fanout = memoryLimit / blockSize;
    const size_t strideSize = (memoryLimit - blockSize) / blockSize;
    blockSize /= sizeof(uint64_t);
    const string tmpFilePath = tmpPath + "/tmp";
    vector<size_t> parts;
    {
        ifstream input(inputPath, std::ios::binary);
        ofstream tmp(tmpFilePath, std::ios::binary);
        vector<uint64_t> data(strideSize * blockSize);
        while (!input.eof()) {
            input.read(reinterpret_cast<char*>(data.data()), blockSize * sizeof(uint64_t));
            size_t readSize = input.gcount() / sizeof(uint64_t);
            std::sort(data.begin(), data.begin() + readSize);
            tmp.write(reinterpret_cast<char*>(data.data()), readSize * sizeof(uint64_t));
            parts.push_back(readSize);
        }
    }
    {
        ifstream input(tmpFilePath, std::ios::binary);
        ofstream output(outputPath, std::ios::binary);
        size_t offset = 0;
        vector<TBlockReader> readers;
        vector<TBlockPointer> heap;
        for (size_t partSize : parts) {
            readers.emplace_back(input, partSize, blockSize, offset);
            heap.emplace_back(readers.size() - 1, 0, readers.back().Block[0]);
            offset += partSize;
            if (readers.size() >= fanout) {
                make_heap(heap);
                while (!heap.empty()) {
                    // do stuff
                }
            }
        }
    }
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
    size_t memoryLimit, blockSize;
    {
        istringstream is(argv[4]);
        is >> memoryLimit;
    }
    {
        istringstream is(argv[5]);
        is >> blockSize;
    }

    Sort(inputPath, outputPath, tmpPath, memoryLimit, blockSize);

    return 0;
}

