#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>
#include <memory.h>
#include <algorithm>
#include <queue>
#include <cassert>

using namespace std;


struct TBlockReader {
    TBlockReader(ifstream& reader, size_t dataSize, size_t blockSize, size_t startOffset)
        : Reader(reader)
        , DataSize(dataSize)
        , BlockSize(blockSize)
        , Offset(startOffset) {
        LoadBlock();
    }

    void LoadBlock() {
        Pos = 0;
        size_t size = std::min(BlockSize, DataSize);
        Reader.seekg(Offset * sizeof(uint64_t));
        Block.resize(size);
        Reader.read(reinterpret_cast<char*>(Block.data()), size * sizeof(uint64_t));
        Offset += size;
    }

    uint64_t inline Get() {
        return Block[Pos];
    }

    uint64_t Next() {
        size_t value = Get();
        ++Pos;
        if (Pos >= DataSize) {
            Eof = true;
        } else if (Pos >= BlockSize) {
            DataSize -= Block.size();
            LoadBlock();
        }
        return value;
    }

    ifstream& Reader;
    size_t DataSize;
    size_t BlockSize;
    size_t Offset;
    size_t Pos = 0;
    bool Eof = false;
    vector<uint64_t> Block;
};

struct TReaderPointer {
    TReaderPointer(size_t readerIdx, uint64_t value)
        : ReaderIdx(readerIdx)
        , Value(value) {}

    bool operator<(const TReaderPointer& other) const {
        return Value > other.Value;
    }

    size_t ReaderIdx;
    uint64_t Value;
};

struct TBlockWriter {
    TBlockWriter(ofstream& writer, size_t blockSize)
        : Writer(writer)
        , BlockSize(blockSize)
        , Block(blockSize) {
    }

    void DumpBlock() {
        Writer.write(reinterpret_cast<char*>(Block.data()), Pos * sizeof(uint64_t));
        Pos = 0;
    }

    void Write(uint64_t value) {
        Block[Pos] = value;
        ++Pos;
        if (Pos >= BlockSize) {
            DumpBlock();
        }
    }

    ofstream& Writer;
    size_t DataSize;
    size_t BlockSize;
    size_t Offset;
    size_t Pos = 0;
    bool Eof = false;
    vector<uint64_t> Block;
};

void Merge(std::priority_queue<TReaderPointer>& heap, std::vector<TBlockReader>& readers, TBlockWriter& writer) {
    while (!heap.empty()) {
        auto el = heap.top();
        writer.Write(el.Value);
        heap.pop();
        auto& reader = readers[el.ReaderIdx];
        if (!reader.Eof) {
            heap.emplace(el.ReaderIdx, reader.Next());
        }
    }
    readers.clear();
}
void Sort(string inputPath, string outputPath, string tmpPath, size_t memoryLimit, size_t blockSize)
{
    memoryLimit /= sizeof(uint64_t);
    blockSize /= sizeof(uint64_t);
    const size_t fanout = memoryLimit / blockSize - 1;
    const std::vector<string> tmpFilePaths = {tmpPath + "/tmp1", tmpPath + "/tmp2"};
    vector<size_t> parts;
    {
        ifstream input(inputPath, std::ios::binary);
        ofstream tmp(tmpFilePaths[0], std::ios::binary);
        vector<uint64_t> data(memoryLimit);
        while (!input.eof()) {
            input.read(reinterpret_cast<char*>(data.data()), memoryLimit * sizeof(uint64_t));
            size_t readSize = input.gcount() / sizeof(uint64_t);
            if (readSize) {
                std::sort(data.begin(), data.begin() + readSize);
                tmp.write(reinterpret_cast<char*>(data.data()), readSize * sizeof(uint64_t));
                parts.push_back(readSize);
            }
        }
    }
    bool direction = false;
    while (parts.size() > 1) {
        std::cerr << parts.size() << std::endl;
        vector<size_t> newParts;
        ifstream input(tmpFilePaths[direction], std::ios::binary);
        ofstream output(tmpFilePaths[!direction], std::ios::binary);
        size_t readOffset = 0;
        size_t writeOffset = 0;
        vector<TBlockReader> readers;
        std::priority_queue<TReaderPointer> heap;
        TBlockWriter writer(output, blockSize);
        size_t newPartSize = 0;
        for (size_t partSize : parts) {
            readers.emplace_back(input, partSize, blockSize, readOffset);
            heap.emplace(readers.size() - 1, readers.back().Next());
            readOffset += partSize;
            newPartSize += partSize;
            if (readers.size() >= fanout) {
                Merge(heap, readers, writer);
                newParts.push_back(newPartSize);
                newPartSize = 0;
            }
        }
        if (!readers.empty()) {
            Merge(heap, readers, writer);
            newParts.push_back(newPartSize);
        }
        writer.DumpBlock();
        direction = !direction;
        parts = std::move(newParts);
        newParts.clear();
    }
    std::rename(tmpFilePaths[direction].c_str(), outputPath.c_str());
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

