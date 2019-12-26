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
#include <fcntl.h>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>

using namespace std;

const size_t PAGE_SIZE = 4 * 1024;

struct TBlock {
    TBlock(const TBlock& _) = delete;
    TBlock(TBlock&& other) {
        Storage = other.Storage;
        Block = other.Block;
        BlockSize = other.BlockSize;
        other.Storage = nullptr;
    }
    
    TBlock() {}

    TBlock(size_t blockSize)
        : BlockSize(blockSize) {
        Storage = std::malloc(blockSize * sizeof(uint64_t) + 512); 
        Block = reinterpret_cast<size_t*>((((size_t)Storage >> 9) + 1) << 9);
    }

    ~TBlock() {
        if (Block != nullptr) {
            std::free(Storage);
        }
    }

    size_t BlockSize;
    size_t* Block = nullptr;
    void* Storage = nullptr;
};

struct TFileInput {
    TFileInput(const TFileInput& _) = delete;
    TFileInput(TFileInput&& other) {
        desc = other.desc;
        Eof = other.Eof;
        other.desc = -1;
    }

    TFileInput(const string& filename) {
        desc = open(filename.c_str(), O_RDONLY | O_DIRECT);
        if (desc < 0) {
            exit(1);
        }
    }

    ~TFileInput() {
        if (desc > -1 && close(desc) < 0) {
            exit(1);
        }
    }

    int Read(uint64_t* buffer, size_t size) {
        int c = read(desc, reinterpret_cast<char*>(buffer), size * sizeof(uint64_t));
        if (c < 0) {
            exit(1);
        }
        if (c < size) {
            Eof = true;
        }
        return c / sizeof(uint64_t);
    }

    bool eof() const {
        return Eof;
    }
    
    bool Eof = false;
    int desc = -1;
};

struct TFileOutput {
    TFileOutput(const TFileOutput& _) = delete;
    TFileOutput(TFileOutput&& other) {
        desc = other.desc;
        other.desc = -1;
    }

    TFileOutput(const string& filename) {
        desc = open(filename.c_str(), O_WRONLY | O_CREAT | O_DIRECT, 0664);
        if (desc < 0) {
            exit(1);
        }
    }

    ~TFileOutput() {
        if (desc > -1 && close(desc) < 0) {
            exit(1);
        }
    }

    void Write(uint64_t* buffer, size_t size) {
        int c = write(desc, reinterpret_cast<char*>(buffer), size * sizeof(uint64_t));
        if (c != size * sizeof(uint64_t)) {
            exit(1);
        }
    }
    
    int desc = -1;
};

struct TBlockReader : TBlock {
    TBlockReader(const string& fname, size_t dataSize, size_t blockSize)
        : TBlock(blockSize)
        , DataSize(dataSize)
        , Reader(fname) {
        LoadBlock();
    }

    void LoadBlock() {
        Pos = 0;
        ReadSize = Reader.Read(Block, BlockSize);
        if (ReadSize == 0) {
            Eof = true;
        }
    }

    uint64_t inline Get() {
        return Block[Pos];
    }

    uint64_t Next() {
        size_t value = Get();
        ++Pos;
        --DataSize;
        if (DataSize == 0) {
            Eof = true;
        } else if (Pos >= ReadSize) {
            if (Reader.eof()) {
                Eof = true;
            } else {
                LoadBlock();
            }
        }
        return value;
    }

    TFileInput Reader;
    size_t Pos = 0;
    size_t DataSize;
    size_t ReadSize;
    bool Eof = false;
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

struct TBlockWriter : TBlock {
    TBlockWriter(const string& fname, size_t blockSize)
        : TBlock(blockSize)
        , Writer(fname) {
    }

    void DumpBlock() {
        Writer.Write(Block, BlockSize);
        Pos = 0;
    }

    void Write(uint64_t value) {
        Block[Pos] = value;
        ++Pos;
        if (Pos >= BlockSize) {
            DumpBlock();
        }
    }

    TFileOutput Writer;
    size_t Pos = 0;
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
    memoryLimit = (memoryLimit >> 10) << 10;
    std::cerr << memoryLimit << std::endl;
    memoryLimit /= sizeof(uint64_t);
    blockSize = ((blockSize >> 9) + 1) << 9;
    std::cerr << blockSize << std::endl;
    blockSize /= sizeof(uint64_t);
    const size_t fanout = memoryLimit / blockSize - 1;
    memoryLimit >>= 1;
    std::cerr << "fanout: " << fanout << std::endl;
    std::priority_queue<TPartMeta> parts;
    size_t nextIdx = 0;
    {
        TFileInput input(inputPath);
        TBlock buffer(memoryLimit);
        while (!input.eof()) {
            size_t partIdx = nextIdx++;
            size_t readSize = input.Read(buffer.Block, memoryLimit);
            if (readSize) {
                TFileOutput tmp(GetTmpPath(tmpPath, partIdx));
                std::sort(buffer.Block, buffer.Block + readSize);
                tmp.Write(buffer.Block, memoryLimit);
                parts.emplace(partIdx, readSize);
            }
        }
    }
    std::cerr << "merge phase" << std::endl;
    while (parts.size() > 1) {
        std::cerr << "part num " << parts.size() << std::endl;
        vector<TBlockReader> readers;
        readers.reserve(fanout);
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
            readers.emplace_back(fileName, partMeta.Size, blockSize);
            heap.emplace(readers.size() - 1, readers.back().Next());
            newPartSize += partMeta.Size;
        }
        std::cerr << newPartSize << std::endl;
        size_t partIdx = nextIdx++;
        TBlockWriter writer(GetTmpPath(tmpPath, partIdx), blockSize);
        Merge(heap, readers, writer);
        for (string& fileName : fileNames) {
            std::remove(fileName.c_str());
        }
        parts.emplace(partIdx, newPartSize);
    }
    std::rename(GetTmpPath(tmpPath, nextIdx - 1).c_str(), outputPath.c_str());
    int res = std::system(("truncate -s " + std::to_string(parts.top().Size * sizeof(uint64_t)) + " " + outputPath).c_str());
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

