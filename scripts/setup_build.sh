#!/bin/bash

# setup build mode
if [ $# == 0 ] || [ "$1" == "Release" ]; then
    echo "Build with Release mode"
    BUILD_FOLDER="build"
    BUILD_TYPE="Release"
elif [ "$1" == "RelWithDebInfo" ]; then
    BUILD_FOLDER="build-reldbg"
    BUILD_TYPE="RelWithDebInfo"
elif [ "$1" == "Debug" ]; then
    echo "Build with Debug mode"
    BUILD_FOLDER="build-debug"
    BUILD_TYPE="Debug"
else
    echo "Invalid build mode: $1"
    exit 1
fi

if [ $# -ge 2 ]; then
    if [ "$2" == "ASAN" ]; then
        BUILD_FOLDER="${BUILD_FOLDER}-asan"
        SANITIZE_ARGS="-DSANITIZE=address"
    elif [ "$2" == "TSAN" ]; then
        BUILD_FOLDER="${BUILD_FOLDER}-tsan"
        SANITIZE_ARGS="-DSANITIZE=thread"
    elif [ "$2" == "MSAN" ]; then
        BUILD_FOLDER="${BUILD_FOLDER}-msan"
        SANITIZE_ARGS="-DSANITIZE=memory"
    elif [ "$2" == "UBSAN" ]; then
        BUILD_FOLDER="${BUILD_FOLDER}-ubsan"
        SANITIZE_ARGS="-DSANITIZE=undefined"
    else
        echo "Invalid sanitizer mode: $2"
        exit 1
    fi
fi
