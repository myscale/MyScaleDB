cmake_minimum_required(VERSION 3.22)

project(SUPERCRATE_INTEGRATE)

file(GLOB INTEGRATE_ENTRY ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)
file(GLOB INTEGRATE ${CMAKE_CURRENT_SOURCE_DIR}/*)

message(STATUS "Config Google Benchmark")

add_executable(supercrate_integrate ${INTEGRATE_ENTRY} ${INTEGRATE})
target_include_directories(supercrate_integrate PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(supercrate_integrate
    PUBLIC
    ch_rust::supercrate
)
if(APPLE)
    target_link_libraries(supercrate_integrate
        PUBLIC
        "-framework CoreFoundation"
    )
endif()