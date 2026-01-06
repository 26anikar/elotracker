cmake_minimum_required(VERSION 3.5)
project(elotracker_tools CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost REQUIRED COMPONENTS filesystem date_time system)
find_package(nlohmann_json REQUIRED)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/copy
    /mnt/disks/data2/libnop/include
)

set(COMMON_LIBS
    rocksdb
    ${Boost_LIBRARIES}
    nlohmann_json::nlohmann_json
    ssl
    crypto
    pthread
    dl
    zstd
    bz2
    snappy
    lz4
    glog
    gflags
    z
    uring
)

add_executable(copytorocks copy/copytorocks.cpp)
target_link_libraries(copytorocks ${COMMON_LIBS})

add_executable(copy_latest_uscf_supplement copy/copy_latest_uscf_supplement.cpp)
target_link_libraries(copy_latest_uscf_supplement ${COMMON_LIBS})

add_executable(copy_latest_fide_supplement copy/copy_latest_fide_supplement.cpp)
target_link_libraries(copy_latest_fide_supplement ${COMMON_LIBS})

add_executable(copyfidesupplement copy/copyfidesupplement.cpp)
target_link_libraries(copyfidesupplement ${COMMON_LIBS})

add_executable(copysupplement copy/copysupplement.cpp)
target_link_libraries(copysupplement ${COMMON_LIBS})

add_executable(copy_uscf_top_players_rocks copy/copy_uscf_top_players_rocks.cpp)
target_link_libraries(copy_uscf_top_players_rocks ${COMMON_LIBS})

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/build)
