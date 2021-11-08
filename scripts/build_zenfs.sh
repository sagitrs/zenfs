#!/bin/bash

BASE=$PWD
OUTPUT=build
METRICS_PATH=$1
mkdir -p $OUTPUT

echo $METRICS_PATH
if test -z "$METRICS_PATH";then
		echo "cmake_metrics2 path is required, usage: ./build.sh [metrics_path]"
		exit
fi

cd $BASE/$OUTPUT && cmake ../ -DCMAKE_INSTALL_PREFIX=$OUTPUT \
																														-DCMAKE_BUILD_TYPE=RelWithDebInfo \
																														-DWITH_TOOLS=ON \
																														-DWITH_TERARK_ZIP=OFF \
																														-DWITH_BYTEDANCE_METRICS=ON \
																														-DBYTEDANCE_METRICS_PATH=$METRICS_PATH \
																														-DWITH_ZENFS=ON

cd $BASE/$OUTPUT && make -j $(nproc) && make install
