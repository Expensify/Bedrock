FROM phusion/baseimage:0.11 AS builder

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y software-properties-common \
  && add-apt-repository -y ppa:ubuntu-toolchain-r/test \
  && apt-get update \
  && apt-get upgrade -y \
  && apt-get install -y gcc-9 g++-9 libpcre3-dev libpcre++-dev zlib1g-dev make gdb git \
  && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 60 --slave /usr/bin/g++ g++ /usr/bin/g++-9

RUN adduser --disabled-password --gecos "" build \
  && adduser build build \
  && chgrp build /lib \
  && chmod 775 /lib

RUN mkdir /src
COPY ./ /src

RUN chown -R build:build /src

USER build
WORKDIR /src

ARG CC=gcc-9
ARG GXX=g++-9 

# prevent stray build artifacts from the COPY stopping a clean build
RUN make clean
RUN make -j8

# tests

WORKDIR /src/test
RUN ./test -threads 8
WORKDIR /src/test/clustertest
RUN ./clustertest -threads 8

#### build complete, now to make run image

FROM phusion/baseimage:0.11

ARG DEBIAN_FRONTEND=noninteractive

# GCC-9 needs to be installed to run dynamically linked libstdc++ binaries like BedrockDB
RUN apt-get update && apt-get install -y software-properties-common \
  && add-apt-repository -y ppa:ubuntu-toolchain-r/test \
  && apt-get update \
  && apt-get upgrade -y \
  && apt-get install -y gcc-9 g++-9 libpcre3 libpcre++ zlib1g \
  && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 60 --slave /usr/bin/g++ g++ /usr/bin/g++-9

COPY --from=builder /src/bedrock /usr/local/bin/
RUN mkdir -p /etc/service/bedrock/
COPY --from=builder /src/bedrock.sh /etc/service/bedrock/run
RUN chmod +x /etc/service/bedrock/run

RUN adduser --disabled-password --gecos "" bedrock \
  && adduser bedrock bedrock \
  && mkdir /db \
  && chown -R bedrock:bedrock /db

CMD ["/sbin/my_init"]

