cmake_minimum_required(VERSION 3.5)
project(elotracker CXX)

include(CheckIncludeFileCXX)

check_include_file_cxx(any HAS_ANY)
check_include_file_cxx(string_view HAS_STRING_VIEW)
check_include_file_cxx(coroutine HAS_COROUTINE)
if (NOT "${CMAKE_CXX_STANDARD}" STREQUAL "")
elseif (HAS_ANY AND HAS_STRING_VIEW AND HAS_COROUTINE)
    set(CMAKE_CXX_STANDARD 20)
elseif (HAS_ANY AND HAS_STRING_VIEW)
    set(CMAKE_CXX_STANDARD 17)
else ()
    set(CMAKE_CXX_STANDARD 14)
endif ()

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(${PROJECT_NAME} main.cc)

target_include_directories(${PROJECT_NAME} PRIVATE /mnt/disks/data2/libnop/include)

set(ROCKSDB_ROOT "/mnt/disks/data2/rocksdb")
target_include_directories(${PROJECT_NAME} PRIVATE ${ROCKSDB_ROOT}/include)
target_link_directories(${PROJECT_NAME} PRIVATE ${ROCKSDB_ROOT}/build)
target_link_libraries(${PROJECT_NAME} PRIVATE ${ROCKSDB_ROOT}/build/librocksdb.a)

find_package(Drogon CONFIG REQUIRED)
target_link_libraries(${PROJECT_NAME} PRIVATE Drogon::Drogon)
target_link_libraries(${PROJECT_NAME} PRIVATE zstd)
target_link_libraries(${PROJECT_NAME} PRIVATE bz2)
target_link_libraries(${PROJECT_NAME} PRIVATE lz4)
target_link_libraries(${PROJECT_NAME} PRIVATE snappy)
target_link_libraries(${PROJECT_NAME} PRIVATE uring)
target_link_libraries(${PROJECT_NAME} PRIVATE fmt)
target_link_libraries(${PROJECT_NAME} PRIVATE glog)
target_link_libraries(${PROJECT_NAME} PRIVATE gflags)

if (CMAKE_CXX_STANDARD LESS 17)
    message(FATAL_ERROR "c++17 or higher is required")
elseif (CMAKE_CXX_STANDARD LESS 20)
    message(STATUS "use c++17")
else ()
    message(STATUS "use c++20")
endif ()

aux_source_directory(controllers CTL_SRC)
aux_source_directory(filters FILTER_SRC)
aux_source_directory(plugins PLUGIN_SRC)
aux_source_directory(models MODEL_SRC)

drogon_create_views(${PROJECT_NAME} ${CMAKE_CURRENT_SOURCE_DIR}/views
                    ${CMAKE_CURRENT_BINARY_DIR})

target_include_directories(${PROJECT_NAME}
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
                                   ${CMAKE_CURRENT_SOURCE_DIR}/models)
target_sources(${PROJECT_NAME}
               PRIVATE
               ${SRC_DIR}
               ${CTL_SRC}
               ${FILTER_SRC}
               ${PLUGIN_SRC}
               ${MODEL_SRC})

add_subdirectory(test)
