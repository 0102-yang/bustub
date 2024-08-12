FROM ubuntu:24.04

COPY ./build_support/packages.sh /tmp/packages.sh

RUN /tmp/packages.sh -y \
    rm -rf /var/lib/apt/lists/* \
    rm -rf /tmp/packages.sh