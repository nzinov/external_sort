cmake_minimum_required(VERSION 2.8)

add_executable(main main.cpp)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS} -std=c++14")

find_package(STXXL REQUIRED)
include_directories(${STXXL_INCLUDE_DIRS})
target_link_libraries(main ${STXXL_LIBRARIES})

if(APPLE) 
    find_package(OPENMP REQUIRED)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OPENMP_CXX_FLAGS} -I/usr/local/opt/libomp/include")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L/usr/local/opt/libomp/lib -lomp")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")
endif()
