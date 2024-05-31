#!/bin/bash
set -e

export CXX=g++-13
export CC=gcc-13

# Add the current working directory to $PATH so that tests can find bedrock.
export PATH=$PATH:`pwd`

# Configure ccache settings for travis.
export PATH=/usr/lib/ccache:$PATH
/usr/sbin/update-ccache-symlinks

export CCACHE_COMPILERCHECK="mtime"

# We have include_file_ctime and include_file_mtime since travis never modifies the header file during execution
# and travis shouldn't care about ctime and mtime between new branches.
export CCACHE_SLOPPINESS="pch_defines,time_macros,include_file_ctime,include_file_mtime"
export CCACHE_MAXSIZE="1G"

# ccache recommends a compression level of 5 or less for faster compilations.
# Compression speeds up the tar and untar of the cache between travis runs.
export CCACHE_COMPRESS="true"
export CCACHE_COMPRESSLEVEL="1"

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

travis_fold start install_packages
travis_time_start

if [[ -z "${APT_MIRROR_PASSWORD}" ]]; then
    echo "Running on a fork, using public apt mirror"
    sudo -E apt-add-repository -y "ppa:ubuntu-toolchain-r/test"
else
    echo "Not running a fork, using private apt mirror"
    sudo openssl aes-256-cbc -K $encrypted_f9e02b3c1033_key -iv $encrypted_f9e02b3c1033_iv -in expensify.ca.crt.enc -out /usr/local/share/ca-certificates/expensify.ca.crt -d
    sudo update-ca-certificates
    sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys BA9EF27F
    echo "deb [arch=amd64] https://travis:$APT_MIRROR_PASSWORD@apt-mirror.expensify.com:843/mirror/ppa.launchpad.net/ubuntu-toolchain-r/test/ubuntu focal main" | sudo tee -a /etc/apt/sources.list
fi

sudo apt-get update -y
sudo -E apt-get -yq --no-install-suggests --no-install-recommends $(travis_apt_get_options) install gcc-13 g++-13

travis_time_finish
travis_fold end build_bedrock

# don't print out versions until after they are installed
${CC} --version
${CXX} --version

travis_fold start build_bedrock
travis_time_start
make -j32
travis_time_finish
travis_fold end build_bedrock

travis_fold start test_bedrock
travis_time_start
cd test
./test -threads 32
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
