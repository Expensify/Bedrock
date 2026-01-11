#!/bin/bash

# Fixes style of modified .cpp and .h files in the current git branch

cd /vagrant/Bedrock/
git fetch origin main # Make sure local references of main branch are up to date
for FILENAME in $(git diff --name-only origin/main...`git branch | grep \* | cut -d ' ' -f2` -- '*.cpp' '*.h'); do
    case "$FILENAME" in
        *tpunit++.cpp|*qrf.h|*sqlite3.h|*sqlite3ext.h)
            continue
            ;;
    esac
    uncrustify -c /vagrant/Bedrock/.uncrustify.cfg -f $FILENAME -o $FILENAME -l CPP;
    # Removes occurrences of std:: that aren't in comments and have a leading space (except if it's inside pointer brackets, eg: <std::thing>)
    sed '/^.*\/\/.*/!s/ std::/ /; /^.* std::.*\/\//s/ std::/ /; /^.*\<.*std::.*\>/s/std:://;' "$FILENAME" > "$FILENAME.new";
    mv -f "$FILENAME.new" "$FILENAME"
done
