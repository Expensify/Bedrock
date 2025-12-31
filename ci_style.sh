#!/bin/bash
set -e

LOCAL_PATH=$(pwd)
git config --global --add safe.directory $LOCAL_PATH

# If the GITHUB_BRANCH env variable is not set to main, fetch the main branch so we can compare against it
if [[ "$GITHUB_BRANCH" != "main" ]]; then
    git fetch origin main:main
fi

# Run 'git fetch origin' so that we have a local branch called $GITHUB_BRANCH that we can compare against
# Only if it starts with pull/
if [[ "$GITHUB_BRANCH" == pull/* ]]; then
    # We set --update-head-ok so that if the branch already exists, it will update the branch to the latest commit,
    # even if head is already pointing to it.
    git fetch origin "${GITHUB_BRANCH}":"${GITHUB_BRANCH}" --update-head-ok

    # IF this failed, exit early
    if [[ $? -ne 0 ]]; then
        exit 1
    fi
fi

EXIT_VAL=0
RED="\u001b[31m"
GREEN="\u001b[32m"
RESET="\u001b[0m"

while read STATUS FILENAME
do
    # Skip some files we don't want to style
    case "$FILENAME" in
        *tpunit++.cpp|*qrf.h|*sqlite3.h|*sqlite3ext.h)
            # Match found: do nothing and continue the loop
            continue
            ;;
    esac

    FAILED="false"

    # Allow failure so we can capture exit status
    set +e
    OUT=$(uncrustify --check -c ./.uncrustify.cfg -f ${FILENAME} -l CPP 2>&1 | grep -e ^FAIL -e ^PASS | awk '{ print $2 }'; exit ${PIPESTATUS[0]})
    RETURN_VAL=$?

    # Stop allowing failure again after we have captured
    set -e

    if [[ $RETURN_VAL -gt 0 ]]; then
        echo -e "${RED}${OUT} failed style checks.${RESET}"
        FAILED="true"
        EXIT_VAL=$RETURN_VAL
    else
        echo -e "${GREEN}${OUT} passed style checks.${RESET}"
    fi

    # Counts occurrences of std:: that aren't in comments and have a leading space (except if it's inside pointer brackets, eg: <std::thing>)
    RETURN_VAL=$(sed -n '/^.*\/\/.*/!s/ std:://p; /^.* std::.*\/\//s/ std:://p; /^.*\<.*std::.*\>/s/std:://p;' "${FILENAME}" | wc -l)
    if [[ $RETURN_VAL -gt 0 ]] && [[ "$FAILED" != "true" ]]; then
        echo -e "${RED}${OUT} failed style checks, do not use std:: prefix.${RESET}"
        EXIT_VAL=$RETURN_VAL
    fi

    # Counts occurrences of include <regex> and fails if it finds one since std:: regex has bad performance
    RETURN_VAL=$(grep -R '^\#include <regex>' "${FILENAME}" | wc -l)
    if [[ $RETURN_VAL -gt 0 ]] && [[ "$FAILED" != "true" ]] && [[ "$FILENAME" != *tpunit++.cpp ]]; then
        echo -e "${RED}${OUT} failed style checks, do not use regex from std on $FILENAME.${RESET}"
        EXIT_VAL=$RETURN_VAL
    fi

done < <(git diff --name-status --diff-filter=AM main..."${GITHUB_BRANCH}" -- '*.cpp' '*.h')

if [ $EXIT_VAL -gt 0 ]; then
    echo -e "${RED}Style is wrong, run 'vssh ./style.sh' from your Mac, or just './style.sh' from your VM to fix it.${RESET}"
fi

exit $EXIT_VAL
