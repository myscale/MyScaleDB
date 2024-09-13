# Building Myscale on Linux

## Preparing the Linux Environment

Install additional dependencies:
```bash
sudo apt update && apt install git git-lfs build-essential ccache \
curl g++ gcc ninja-build debhelper pbuilder fakeroot alien devscripts gperf rustc cargo \
moreutils pigz pixz debian-archive-keyring debian-keyring yasm --yes
```

Add the LLVM repository:

```bash
export LLVM_VERSION=15
export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')"

wget -q -O - https://apt.llvm.org/llvm-snapshot.gpg.key | \
sudo apt-key add -

echo "deb [trusted=yes] http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" | \
sudo tee -a /etc/apt/sources.list.d/llvm.list

sudo apt update
```

Install Clang 15:

```bash
export LLVM_VERSION=15 && \
apt install llvm-${LLVM_VERSION} \
llvm-${LLVM_VERSION}-dev \
clang-${LLVM_VERSION} \
clang-tidy-${LLVM_VERSION} \
lld-${LLVM_VERSION} \
lldb-${LLVM_VERSION} \
libomp-${LLVM_VERSION}-dev
```

Install a specific version of CMake:

```bash
mkdir ~/app && cd ~/app
export arch=$(uname -m) && wget https://cmake.org/files/v3.22/cmake-3.22.2-linux-$arch.tar.gz
tar -xzvf cmake-3.22.2-linux-$arch.tar.gz

ls ~/app/cmake-3.22.2-linux-$arch/bin

ln -s ~/app/cmake-3.22.2-linux-$arch/bin/cmake /usr/local/bin/cmake
ln -s ~/app/cmake-3.22.2-linux-$arch/bin/cmake /usr/bin/cmake
cmake --version
```

## Preparing the Source Code

Clone the source code repository:

```bash
# clone the repository
git clone https://github.com/myscale/MyScaleDB.git && cd MyScaleDB

# initialize submodules
git submodule update --init --recursive -f && git -C contrib/sysroot lfs pull
```

## Building the Code

```bash
mkdir build && cd build

cmake -G Ninja .. -DCMAKE_C_COMPILER=$(command -v clang-15) \
    -DCMAKE_CXX_COMPILER=$(command -v clang++-15) \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_RUST=ON
```
This guide will help you set up your environment, download the necessary dependencies, and build Myscale on Linux with Clang and Rust support.
