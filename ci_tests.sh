#!/bin/bash
set -e

source ./ci_utils.sh

# If ENABLE_HCTREE is set, add a flag to the test called -enableHctree
if [ "$ENABLE_HCTREE" == "true" ]; then
  ENABLE_HCTREE_FLAG="-enableHctree"
fi

git config --global --add safe.directory `pwd`

# Add bedrock binary to the path
export PATH=$PATH:`pwd`

# Run squid in the background
squid -sYC

export https_proxy=http://127.0.0.1:3128

# Run wget to example.com until it succeeds. Try ten times, else, fail.
for i in {1..10}; do
  if wget -q --spider https://example.com; then
    echo "Squid started"
    break
  fi
  if [ $i -eq 10 ]; then
    echo "Squid failed to start"
    exit 1
  fi
  echo "Waiting for squid to start..."
  sleep 1
done

cd test
mark_fold start test_bedrock
./test -threads 64 $ENABLE_HCTREE_FLAG
mark_fold end test_bedrock

cd clustertest
mark_fold start test_bedrock_cluster
./clustertest -threads 8 $ENABLE_HCTREE_FLAG
mark_fold end test_bedrock_cluster
