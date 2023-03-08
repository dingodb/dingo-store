#/bin/bash

BASE_DIR=$(dirname $(cd $(dirname $0); pwd))

find $BASE_DIR/src -name "*.cc" | xargs  clang-format -style=file -i
find $BASE_DIR/src -name "*.h" | xargs  clang-format -style=file -i

