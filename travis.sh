#!/bin/bash
set -e

export GXX=g++-6
export CC=gcc-6

${CC} --version
${GXX} --version

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

CORES=4

travis_fold start build_bedrock
travis_time_start
make
travis_time_finish
travis_fold end build_bedrock

travis_fold start test_bedrock
travis_time_start
cd test
#./test -threads 8
cd ..
travis_time_finish
travis_fold end test_bedrock

travis_fold start test_bedrock_cluster
travis_time_start
cd test/clustertest
./clustertest -only BadCommand
cd ../..
travis_time_finish
travis_fold end test_bedrock_cluster
