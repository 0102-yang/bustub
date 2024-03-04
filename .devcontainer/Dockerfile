ARG UBUNTU_VERSION=24.04
FROM ubuntu:${UBUNTU_VERSION}

RUN apt-get -y update && \
	apt-get -y upgrade && \
	apt-get -y install --no-install-recommends \
    g++ \
    gdb \
    clang-14 \
    clang-format-14 \
    clang-tidy-14 \
    cmake \
    ninja-build \
    doxygen \
    git \
    pkg-config \
    zlib1g-dev \
    libelf-dev \
    libdwarf-dev
