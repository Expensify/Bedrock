#!/bin/bash
set -e

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

export ENABLE_HCTREE=${ENABLE_HCTREE:-"false"}
export PATH=$PATH:`pwd`

git config --global --add safe.directory `pwd`

# Run squid in the background
squid -sYC

cd test
mark_fold start test_bedrock
./test -threads 64
mark_fold end test_bedrock

cd clustertest
mark_fold start test_bedrock_cluster
./clustertest -threads 8
mark_fold end test_bedrock_cluster
