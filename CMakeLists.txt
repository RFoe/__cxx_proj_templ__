cmake_minimum_required (VERSION 3.8...4.0.0 FATAL_ERROR)
cmake_policy (SET CMP0155 NEW)

set (PROJECT_DESCRIPTION "__lru__" CACHE STRING "project description")
set_property (GLOBAL PROPERTY USE_FOLDERS ON)

set (CMAKE_BUILD_TYPE Debug CACHE STRING "build type" FORCE)

# set (VCPKG_TARGET_TRIPLET x64-linux CACHE STRING "VCPKG target triplet" FORCE)
set (VCPKG_TARGET_TRIPLET x64-linux-clang-libcxx
     CACHE STRING "VCPKG target triplet" FORCE
)

set(__SAN__ -fsanitize=thread,undefined)

project (__lru VERSION 0.1.0 LANGUAGES CXX C DESCRIPTION ${PROJECT_DESCRIPTION})

enable_testing ()

set (CMAKE_CXX_STANDARD 26)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
set (CMAKE_CXX_EXTENSIONS OFF)

execute_process (
    COMMAND nproc OUTPUT_VARIABLE NPROC OUTPUT_STRIP_TRAILING_WHITESPACE
)
set (CMAKE_BUILD_PARALLEL_LEVEL ${NPROC})

find_program (CCACHE_FOUND ccache)
if (CCACHE_FOUND)
    set (CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_FOUND})
endif ()

find_package (Catch2 CONFIG REQUIRED)
find_package (benchmark CONFIG REQUIRED)

file (
    GLOB_RECURSE
    __SOURCES__
    CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/source/*.c
    ${CMAKE_CURRENT_SOURCE_DIR}/source/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/source/*.cu
    ${CMAKE_CURRENT_SOURCE_DIR}/source/*.cxx
)

file (
    GLOB_RECURSE
    __TEST_SOURCES__
    CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/test/*.c
    ${CMAKE_CURRENT_SOURCE_DIR}/test/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/test/*.cu
    ${CMAKE_CURRENT_SOURCE_DIR}/test/*.cxx
)

file (
    GLOB_RECURSE
    __BENCHMARK_SOURCES__
    CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/*.c
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/*.cu
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/*.cxx
)

add_executable (${PROJECT_NAME} ${__SOURCES__})
target_include_directories (
    ${PROJECT_NAME}
    PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
            $<BUILD_INTERFACE:$ENV{HOME}/git-repositories/stdexec/include>
            $<INSTALL_INTERFACE:include> source ${PROJECT_INCLUDE_DIRS}
)


target_compile_options (
    ${PROJECT_NAME}
    PRIVATE $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall
            -Wextra
            -pedantic
            -Werror>
            $<$<CXX_COMPILER_ID:MSVC>:/W4
            /WX>
            $<$<CONFIG:Debug>:-g
            -fno-omit-frame-pointer
            ${__SAN__}
            >
            $<$<CONFIG:Release>:-O3
            -march=native>
            -stdlib=libc++
)
target_link_options (
    ${PROJECT_NAME} PRIVATE     
    $<$<CONFIG:Debug>:${__SAN__}> 
    -stdlib=libc++ -lc++ -lc++abi
)

if (__TEST_SOURCES__)
    set (__SOURCES_MAIN_EXCLUDED__ ${__SOURCES__})
    list (FILTER __SOURCES_MAIN_EXCLUDED__ EXCLUDE REGEX ".*main\\.cxx$")
    add_executable (
        test_${PROJECT_NAME} ${__TEST_SOURCES__} ${__SOURCES_MAIN_EXCLUDED__}
    )
    target_include_directories (
        test_${PROJECT_NAME}
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
            $<BUILD_INTERFACE:$ENV{HOME}/git-repositories/stdexec/include>
            $<INSTALL_INTERFACE:include> source ${PROJECT_INCLUDE_DIRS}
    )
    target_link_libraries (
        test_${PROJECT_NAME} PRIVATE Catch2::Catch2 Catch2::Catch2WithMain
    )

    target_compile_options (
        test_${PROJECT_NAME}
        PRIVATE $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall
                -Wextra
                -pedantic
                -Werror
                -Wno-c2y-extensions>
                $<$<CXX_COMPILER_ID:MSVC>:/W4
                /WX>
                $<$<CONFIG:Debug>:-g
                -fno-omit-frame-pointer
                ${__SAN__}
                >
                $<$<CONFIG:Release>:-O3
                -march=native>
                -stdlib=libc++
    )
    target_link_options (
        test_${PROJECT_NAME} PRIVATE    
        $<$<CONFIG:Debug>:${__SAN__}> 
        -stdlib=libc++ -lc++ -lc++abi
    )
    add_test (NAME TEST_${PROJECT_NAME} COMMAND test_${PROJECT_NAME})
    set_tests_properties (TEST_${PROJECT_NAME} PROPERTIES FAIL_ON_STDOUT true)
endif ()

if (__BENCHMARK_SOURCES__)
    set (__SOURCES_MAIN_EXCLUDED__ ${__SOURCES__})
    list (FILTER __SOURCES_MAIN_EXCLUDED__ EXCLUDE REGEX ".*main\\.cxx$")
    add_executable (
        benchmark_${PROJECT_NAME} ${__BENCHMARK_SOURCES__}
                                  ${__SOURCES_MAIN_EXCLUDED__}
    )
    target_include_directories (
        benchmark_${PROJECT_NAME}
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
            $<BUILD_INTERFACE:$ENV{HOME}/git-repositories/stdexec/include>
            $<INSTALL_INTERFACE:include> source ${PROJECT_INCLUDE_DIRS}
    )
    
    target_link_libraries (
        benchmark_${PROJECT_NAME} PRIVATE benchmark::benchmark
                                          benchmark::benchmark_main
    )
    target_compile_options (
        benchmark_${PROJECT_NAME}
        PRIVATE $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall
                -Wextra
                -pedantic
                -Werror>
                $<$<CXX_COMPILER_ID:MSVC>:/W4
                /WX>
                $<$<CONFIG:Debug>:-g
                -fno-omit-frame-pointer
                ${__SAN__}
                >
                $<$<CONFIG:Release>:-O3
                -march=native>
                -stdlib=libc++
    )
    target_link_options (
        benchmark_${PROJECT_NAME} PRIVATE     
        $<$<CONFIG:Debug>:${__SAN__}> 
        -stdlib=libc++ -lc++ -lc++abi
    )
    add_test (NAME BENCHMARK_${PROJECT_NAME} COMMAND benchmark_${PROJECT_NAME})
    set_tests_properties (
        BENCHMARK_${PROJECT_NAME} PROPERTIES FAIL_ON_STDOUT true
    )
endif ()
