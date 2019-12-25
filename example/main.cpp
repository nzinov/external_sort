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
    TBlockReader(ifstream reader, size_t blockSize)
        : Reader(std::move(reader))
        , BlockSize(blockSize)
        , Block(blockSize) {
        LoadBlock();
    }

    void LoadBlock() {
        Pos = 0;
        Reader.read(reinterpret_cast<char*>(Block.data()), BlockSize * sizeof(uint64_t));
        size_t readSize = Reader.gcount() / sizeof(uint64_t);
        Block.resize(readSize);
        if (readSize == 0) {
            Eof = true;
        }
    }

    uint64_t inline Get() {
        return Block[Pos];
    }

    uint64_t Next() {
        size_t value = Get();
        ++Pos;
        if (Pos >= Block.size()) {
            if (Reader.eof()) {
                Eof = true;
            } else {
                LoadBlock();
            }
        }
        return value;
    }

    ifstream Reader;
    size_t BlockSize;
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

struct TPartMeta {
    TPartMeta(size_t partIdx, size_t size)
        : PartIdx(partIdx)
        , Size(size) {}

    bool operator<(const TPartMeta& other) const {
        return Size > other.Size;
    }

    size_t PartIdx;
    size_t Size;
};

struct TBlockWriter {
    TBlockWriter(ofstream writer, size_t blockSize)
        : Writer(std::move(writer))
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

    ofstream Writer;
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
    writer.DumpBlock();
}

string GetTmpPath(string tmpPath, size_t partIdx) {
    return tmpPath + "/tmp" + std::to_string(partIdx);
}

void Sort(string inputPath, string outputPath, string tmpPath, size_t memoryLimit, size_t blockSize)
{
    memoryLimit /= sizeof(uint64_t);
    blockSize /= sizeof(uint64_t);
    const size_t fanout = memoryLimit / blockSize - 1;
    std::priority_queue<TPartMeta> parts;
    size_t nextIdx = 0;
    {
        ifstream input(inputPath, std::ios::binary);
        vector<uint64_t> data(memoryLimit);
        while (!input.eof()) {
            size_t partIdx = nextIdx++;
            input.read(reinterpret_cast<char*>(data.data()), memoryLimit * sizeof(uint64_t));
            size_t readSize = input.gcount() / sizeof(uint64_t);
            if (readSize) {
                ofstream tmp(GetTmpPath(tmpPath, partIdx), std::ios::binary);
                std::sort(data.begin(), data.begin() + readSize);
                tmp.write(reinterpret_cast<char*>(data.data()), readSize * sizeof(uint64_t));
                parts.emplace(partIdx, readSize);
            }
        }
    }
    while (parts.size() > 1) {
        vector<TBlockReader> readers;
        std::priority_queue<TReaderPointer> heap;
        size_t newPartSize = 0;
        std::vector<string> fileNames;
        for (size_t i = 0; i < fanout; ++i) {
            if (parts.empty()) {
                break;
            }
            auto partMeta = parts.top();
            parts.pop();
            string fileName = GetTmpPath(tmpPath, partMeta.PartIdx);
            fileNames.push_back(fileName);
            readers.emplace_back(ifstream(fileName, std::ios::binary), blockSize);
            heap.emplace(readers.size() - 1, readers.back().Next());
            newPartSize += partMeta.Size;
        }
        std::cerr << newPartSize << std::endl;
        size_t partIdx = nextIdx++;
        TBlockWriter writer(ofstream(GetTmpPath(tmpPath, partIdx), std::ios::binary), blockSize);
        Merge(heap, readers, writer);
        for (string& fileName : fileNames) {
            std::remove(fileName.c_str());
        }
        parts.emplace(partIdx, newPartSize);
    }
    std::rename(GetTmpPath(tmpPath, nextIdx - 1).c_str(), outputPath.c_str());
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

