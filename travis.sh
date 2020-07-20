#!/bin/bash
set -e

if [[ "${BUILD_PROCESS}" = make ]]; then
  export GXX=g++-9
else
  export CXX=g++-9
fi
export CC=gcc-9

# Install CMake into predefined path (if needed)
HOME_LOCAL=${HOME}/.local
CMAKE_BIN=${HOME_LOCAL}/bin/cmake
CMAKE_VERSION=3.16.6
if [ -f ${CMAKE_BIN} ] &&
   [ -z $(${CMAKE_BIN} --version | grep -q "${CMAKE_VERSION}") ]; then
  echo "Using cached CMake v${CMAKE_VERSION} (${HOME_LOCAL})\r${ANSI_CLEAR}"
else
  OS_TYPE=Linux
  if [[ "${os}" = Darwin ]]; then
    OS_TYPE=Darwin
  fi

  CMAKE_FILENAME=cmake-${CMAKE_VERSION}-${OS_TYPE}-x86_64.tar.gz
  CMAKE_URL=https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/${CMAKE_FILENAME}
  (
    cd /tmp
    echo -en "Downloading CMake v${CMAKE_VERSION} (${URL})\r${ANSI_CLEAR}"
    wget ${CMAKE_URL}
    echo -en "Installing CMake v${CMAKE_VERSION} (${HOME_LOCAL})\r${ANSI_CLEAR}"
    mkdir -p ${HOME_LOCAL}
    tar xzf ${CMAKE_FILENAME} -C ${HOME_LOCAL} --strip 1
    rm ${CMAKE_FILENAME}
  )
fi

${CC} --version
if [[ "${BUILD_PROCESS}" = make ]]; then
  ${GXX} --version
else
  ${CXX} --version
  cmake --version
fi

travis_time_start() {
  travis_timer_id=$(printf %08x $(( RANDOM * RANDOM )))
  travis_start_time=$(travis_nanoseconds)
  echo -en "travis_time:start:$travis_timer_id\r${ANSI_CLEAR}"
}

travis_time_finish() {
  local result=$?
  travis_end_time=$(travis_nanoseconds)
  local duration=$(($travis_end_time-$travis_start_time))
  echo -en "travis_time:end:$travis_timer_id:start=$travis_start_time,finish=$travis_end_time,duration=$duration\r${ANSI_CLEAR}"
  return $result
}

travis_nanoseconds() {
  local cmd="date"
  local format="+%s%N"
  local os=$(uname)

  if hash gdate > /dev/null 2>&1; then
    cmd="gdate" # use gdate if available
  elif [[ "$os" = Darwin ]]; then
    format="+%s000000000" # fallback to second precision on darwin (does not support %N)
  fi

  $cmd -u $format
}

travis_fold() {
  local action=$1
  local name=$2
  echo -en "travis_fold:${action}:${name}\r${ANSI_CLEAR}"
}

if [[ "${BUILD_PROCESS}" = cmake ]]; then
  travis_fold start configure_bedrock
  travis_time_start
  mkdir build
  cd build
  cmake -DENABLE_TESTING=On ..
  travis_time_finish
  travis_fold end configure_bedrock
fi

travis_fold start build_bedrock
travis_time_start
make -j8
travis_time_finish
travis_fold end build_bedrock

travis_fold start test_bedrock
travis_time_start
cd test
./test -threads 8
cd ..
travis_time_finish
travis_fold end test_bedrock

travis_fold start test_bedrock_cluster
travis_time_start
cd test/clustertest
./clustertest -threads 8
cd ../..
travis_time_finish
travis_fold end test_bedrock_cluster

if [[ "${BUILD_PROCESS}" = cmake ]]; then
  cd ..
fi