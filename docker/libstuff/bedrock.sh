#!/bin/bash
set -e

# Requires bash 4.2 or greater because of use of `-v` for testing variables being set
# Returns 1 if version doesn't match
function bashVersionCheck()
{
  [ -z $BASH_VERSION ] && return 1

  # If it's set, check the version
  case $BASH_VERSION in
    5.*) return 0 ;;
    4.1) return 1 ;;
    4.0) return 1 ;;
    4.*) return 0 ;;
    ?) return 1;;
  esac
}

if [[ $(bashVersionCheck) ]]; then
  echo "Requires bash 4.2 or greater"
  exit -1
fi

NO_ARG_PARAMS=("VERBOSE" "QUIET" "CLEAN")
ONE_ARG_PARAMS=("SERVER_HOST" "NODE_NAME" "PEER_LIST" "PRIORITY" "PLUGINS" "CACHE_SIZE" "WORKER_THREADS" "QUERY_LOG" "MAX_JOURNAL_SIZE" "SYNCHRONOUS")

function toLowerCase() {
  echo "$1" | tr '[:upper:]' '[:lower:]'
}

function toSnakeCase()
{
  lowered=$(toLowerCase $1)
  arr=(${lowered//_/ })
  printf -v ccase %s "${arr[@]^}"
  echo "${lowered:0:1}${ccase:1}"
}

PARAMS=""

# Add default database path
DB_PATH="/var/db/bedrock.db"
PARAMS="$PARAMS -db $DB_PATH -nodeHost 0.0.0.0:9000 -serverHost 0.0.0.0:8888"

for i in "${NO_ARG_PARAMS[@]}" ; do
  if [[ -v "$i" ]]; then
    export PARAMS="$PARAMS -$(toLowerCase ${i:0:1})"
  fi
done

for i in "${ONE_ARG_PARAMS[@]}" ; do
  if [[ -v "$i" ]]; then
    export PARAMS="$PARAMS -$(toSnakeCase ${i}) ${!i}"
  fi
done

exec /sbin/setuser bedrock /usr/local/bin/bedrock $PARAMS >> /var/log/bedrock.log 2>&1