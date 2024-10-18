# Stage 1: Build
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install apt-fast and necessary tools
RUN apt-get update && apt-get install -y software-properties-common gnupg wget && \
    add-apt-repository ppa:apt-fast/stable && \
    apt-get update && apt-get -y install apt-fast

# Configure apt-fast
RUN echo debconf apt-fast/maxdownloads string 16 | debconf-set-selections && \
    echo debconf apt-fast/dlflag boolean true | debconf-set-selections && \
    echo debconf apt-fast/aptmanager string apt-get | debconf-set-selections

# Add necessary GPG keys and repositories
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys BA9EF27F && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 4F4EA0AAE5267A6C && \
    add-apt-repository ppa:ubuntu-toolchain-r/test && \
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    echo "deb http://apt.llvm.org/jammy/ llvm-toolchain-jammy-18 main" >> /etc/apt/sources.list.d/llvm.list

# Update and install dependencies
RUN apt-fast update && apt-fast install -y \
    wget gnupg software-properties-common lsb-release ccache zlib1g-dev rsyslog cmake \
    libpcre2-dev libpcre3-dev libsodium-dev libgpgme11-dev libstdc++-13-dev make \
    linux-headers-generic git clang-18 lldb-18 lld-18 clangd-18 clang-tidy-18 \
    clang-format-18 clang-tools-18 llvm-18-dev llvm-18-tools libomp-18-dev libc++-18-dev \
    libc++abi-18-dev libclang-common-18-dev libclang-18-dev libclang-cpp18-dev libunwind-18-dev

# Configure alternatives to use Clang 18 by default
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-18 100 && \
    update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-18 100

# Configure ccache
ENV CCACHE_DIR=/ccache
RUN mkdir -p $CCACHE_DIR && chmod 777 $CCACHE_DIR
ENV PATH="/usr/lib/ccache:$PATH"

WORKDIR /app/bedrock

COPY . /app/bedrock

ENV CC="clang-18"
ENV CXX="clang++-18"

# Build Bedrock
RUN make clean
RUN make -j$(nproc)

# Add Bedrock to the PATH for the tests
ENV PATH="/app/bedrock:$PATH"

# Run tests
WORKDIR /app/bedrock/test
RUN ./test -threads 8
WORKDIR /app/bedrock/test/clustertest
RUN ./clustertest -threads 8

# Clean up
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    rm -rf /etc/apt/sources.list.d/*

# Stage 2: Runtime
FROM phusion/baseimage:noble-1.0.0

ARG DEBIAN_FRONTEND=noninteractive

# Install necessary packages
RUN apt-get update && apt-get install -y software-properties-common && \
    apt-get install -y build-essential libpcre++ zlib1g && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    rm -rf /etc/apt/sources.list.d/*

# Copy the compiled binary from the build stage
COPY --from=builder /app/bedrock/bedrock /usr/local/bin/
RUN chmod +x /usr/local/bin/bedrock

RUN mkdir -p /var/db && \
    touch /var/db/bedrock.db

EXPOSE 8888
EXPOSE 9000

RUN mkdir -p /etc/service/bedrock/
COPY docker/libstuff/bedrock.sh /etc/service/bedrock/run
RUN chmod +x /etc/service/bedrock/run

RUN adduser --disabled-password --gecos "" bedrock && \
    adduser bedrock bedrock && \
    chown -R bedrock:bedrock /var/db

CMD ["/sbin/my_init"]
