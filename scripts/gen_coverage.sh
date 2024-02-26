#/bin/bash

set -e

BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
SRC_DIR=${BASE_DIR}/src
GCOV_OBJ_DIR=/tmp/gcov
LCOV_INFO_FILEPATH=/tmp/lcov/coverage.info
LCOV_OUTPUT_DIR=/tmp/lcov/out

echo "copy gcno and gcda file..."
if [ ! -d ${GCOV_OBJ_DIR} ];then
  mkdir ${GCOV_OBJ_DIR}
fi
find $BASE_DIR/build -name "*.gcno" | grep 'DINGODB_OBJS.dir' | xargs -i mv {} ${GCOV_OBJ_DIR}
find $BASE_DIR/build -name "*.gcda" | grep 'DINGODB_OBJS.dir' | xargs -i mv {} ${GCOV_OBJ_DIR}

echo "generate gcov file..."
cd ${GCOV_OBJ_DIR}
src_filenames=`find ${SRC_DIR} -name "*.cc" | grep -v 'client' | grep -v 'sdk' | xargs -n1 basename`
for filename in ${src_filenames[*]}; do
  gcno_filename=${filename}.gcno
  gcda_filename=${filename}.gcda
  if [ -f ${gcno_filename} -a -f ${gcda_filename} ];then
    gcov ${gcno_filename} --object-directory=${GCOV_OBJ_DIR} --source-prefix=${SRC_DIR} --relative-only
  fi
done

echo "generate material for web report used by lcov..."
if [ -f ${LCOV_INFO_FILEPATH} ];then
  rm -f ${LCOV_INFO_FILEPATH}
fi
lcov --capture --directory ${GCOV_OBJ_DIR} --output-file ${LCOV_INFO_FILEPATH} --include '*src/*'


echo "generate web report..."
if [ -d ${LCOV_OUTPUT_DIR} ];then
  rm -rf ${LCOV_OUTPUT_DIR}
fi
genhtml ${LCOV_INFO_FILEPATH} --title 'dingo-store unit test' --output-directory ${LCOV_OUTPUT_DIR}

echo "starting http server..."
cd ${LCOV_OUTPUT_DIR}
python3 -m http.server 9002