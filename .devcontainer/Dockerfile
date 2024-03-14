ARG UBUNTU_VERSION=24.04
FROM ubuntu:${UBUNTU_VERSION}

RUN apt-get -y update && \
	apt-get -y upgrade && \
	apt-get -y install --no-install-recommends \
    clang-14 \
    libclang-rt-14-dev \
    clang-format-14 \
    clang-tidy-14 \
    cmake \
    gdb \
    git \
    ninja-build \
    doxygen \
    pkg-config \
    zlib1g-dev \
    libelf-dev \
    libdwarf-dev \
    libdw-dev \
    binutils-dev
