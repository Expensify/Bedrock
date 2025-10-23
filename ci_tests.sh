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

# If ENABLE_HCTREE is set, add a flag to the test called -enableHctree
if [ "$ENABLE_HCTREE" == "true" ]; then
  ENABLE_HCTREE_FLAG="-enableHctree"
fi

git config --global --add safe.directory `pwd`

# Run squid in the background
squid -sYC

# Sleep for a few seconds to let squid start
sleep 10

cd test
mark_fold start test_bedrock
./test -threads 64 $ENABLE_HCTREE_FLAG
mark_fold end test_bedrock

cd clustertest
mark_fold start test_bedrock_cluster
./clustertest -threads 8 $ENABLE_HCTREE_FLAG
mark_fold end test_bedrock_cluster
