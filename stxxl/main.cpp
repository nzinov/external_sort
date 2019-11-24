#include <stxxl/io>
#include <stxxl/mng>
#include <stxxl/sort>
#include <stxxl/vector>
#include <stxxl/stream>

#include <omp.h>

#include <sstream>

typedef long long int64;

struct MyInt
{
    int64 key_;

    int64 key() const
    {
        return key_;
    }

    MyInt() { }
    MyInt(int64 k): key_(k) { }

    static int64 min_value()
    {
        return std::numeric_limits<int64>::min();
    }
    static int64 max_value()
    {
        return std::numeric_limits<int64>::max();
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
    if (argc != 2)
    {
        // WRITE_CODE_HERE: consider output and memory limit here.
        std::cout << "Usage: " << argv[0] << " <in>" << std::endl;
        return 1;
    }

    // Disable cpu parallelism.
    omp_set_num_threads(1);

    stxxl::syscall_file fin(argv[1], stxxl::file::DIRECT | stxxl::file::RDONLY);

    // Block size is hardcoded for HDD.
    const stxxl::unsigned_type block_size = 8 * 1024 * 1024;
    typedef stxxl::vector<MyInt, 1, stxxl::lru_pager<8>, block_size> vector_type;

    vector_type input(&fin);

    typedef stxxl::stream::streamify_traits<vector_type::iterator>::stream_type input_stream_type;
    input_stream_type input_stream = stxxl::stream::streamify(input.begin(), input.end());

    // WRITE_CODE_HERE: call stxxl sorting.

    typedef Cmp comparator_type;
    STXXL_MSG((stxxl::is_sorted(input.begin(), input.end(), comparator_type()) ? "OK" : "WRONG"));

    return 0;
}

