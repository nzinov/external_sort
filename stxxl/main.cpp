#include <stxxl/io>
#include <stxxl/mng>
#include <stxxl/sort>
#include <stxxl/vector>
#include <stxxl/stream>

#include <omp.h>

#include <sstream>

struct MyInt
{
    uint64_t key_;

    uint64_t key() const
    {
        return key_;
    }

    MyInt() { }
    MyInt(uint64_t k): key_(k) { }

    static uint64_t min_value()
    {
        return std::numeric_limits<uint64_t>::min();
    }
    static uint64_t max_value()
    {
        return std::numeric_limits<uint64_t>::max();
    }
};

inline bool operator < (const MyInt& lhs, const MyInt& rhs)
{
    return lhs.key() < rhs.key();
}

inline bool operator == (const MyInt& lhs, const MyInt& rhs)
{
    return lhs.key() == rhs.key();
}

struct Cmp
{
    typedef MyInt first_argument_type;
    typedef MyInt second_argument_type;
    typedef bool result_type;
    bool operator () (const MyInt& lhs, const MyInt& rhs) const
    {
        return lhs < rhs;
    }
    static MyInt min_value()
    {
        return MyInt::min_value();
    }
    static MyInt max_value()
    {
        return MyInt::max_value();
    }
};

std::ostream& operator << (std::ostream& o, const MyInt& obj)
{
    o << obj.key();
    return o;
}

int main(int argc, char** argv)
{
    if (argc != 4)
    {
        std::cout << "Usage: " << argv[0] << " <in> <out> <memoryLimit>" << std::endl;
        return 1;
    }

    const stxxl::internal_size_type memoryLimit = atoi(argv[3]);
    const stxxl::internal_size_type blockSize = 4096;

    // Disable cpu parallelism.
    omp_set_num_threads(1);

    stxxl::syscall_file fin(argv[1], stxxl::file::DIRECT | stxxl::file::RDONLY);
    stxxl::syscall_file fout(argv[2], stxxl::file::DIRECT | stxxl::file::RDWR | stxxl::file::CREAT);

    // Block size is hardcoded for HDD.
    const stxxl::unsigned_type block_size = 8 * 1024 * 1024;
    typedef stxxl::vector<MyInt, 1, stxxl::lru_pager<8>, block_size> vector_type;

    vector_type input(&fin);
    vector_type output(&fout);
    output.resize(input.size());

    typedef stxxl::stream::streamify_traits<vector_type::iterator>::stream_type input_stream_type;
    input_stream_type input_stream = stxxl::stream::streamify(input.begin(), input.end());

    typedef Cmp comparator_type;
    typedef stxxl::stream::sort<input_stream_type, Cmp, blockSize> sort_stream_type;
    sort_stream_type sort_stream(input_stream, comparator_type(), memoryLimit);
    vector_type::iterator o = stxxl::stream::materialize(sort_stream, output.begin(), output.end());

    STXXL_MSG((stxxl::is_sorted(output.begin(), output.end(), comparator_type()) ? "OK" : "WRONG"));

    return 0;
}

