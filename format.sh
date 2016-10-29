clang-format -i *.cpp
clang-format -i *.h

cd sqlitecluster
find . -name "*.cpp" | xargs clang-format -i
find . -name "*.h" | xargs clang-format -i
cd ..

cd libstuff
find . -name "*.cpp" | xargs clang-format -i
find . -name "*.h" | xargs clang-format -i
cd ..

cd plugins
find . -name "*.cpp" | xargs clang-format -i
find . -name "*.h" | xargs clang-format -i
cd ..

cd test
find . -name "*.cpp" | xargs clang-format -i
find . -name "*.h" | xargs clang-format -i
cd ..

git checkout libstuff/sqlite3.h
git checkout libstuff/sqlite3ext.h
