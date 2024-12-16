#!/bin/bash
set -e

# Add the current working directory to $PATH so that tests can find bedrock.
export PATH=$PATH:`pwd`

export CCACHE_COMPILERCHECK="mtime"

# We have include_file_ctime and include_file_mtime since travis never modifies the header file during execution
# and gh actions shouldn't care about ctime and mtime between new branches.
export CCACHE_SLOPPINESS="pch_defines,time_macros,include_file_ctime,include_file_mtime"
export CCACHE_MAXSIZE="1G"

# ccache recommends a compression level of 5 or less for faster compilations.
# Compression speeds up the tar and untar of the cache between travis runs.
export CCACHE_COMPRESS="true"
export CCACHE_COMPRESSLEVEL="1"

mark_fold() {
  local action=$1
  local name=$2

  # if action == end, just print out ::endgroup::
  if [[ "$action" == "end" ]]; then
    echo ::endgroup::
    return
  fi

  echo "::group::${name}"
}

export CC="clang-18"
export CXX="clang++-18"

# don't print out versions until after they are installed
${CC} --version
${CXX} --version

mark_fold start build_bedrock
make -j64
mark_fold end build_bedrock

mark_fold start test_bedrock
cd test
./test -threads 64
cd ..
mark_fold end test_bedrock

mark_fold start test_bedrock_cluster
cd test/clustertest
./clustertest -threads 1
cd ../..
mark_fold end test_bedrock_cluster

strip bedrock
