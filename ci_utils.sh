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
