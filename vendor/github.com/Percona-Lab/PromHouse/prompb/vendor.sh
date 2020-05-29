#!/bin/bash

set -ex

rm -fr prometheus
git clone https://github.com/prometheus/prometheus.git

rm -fr master
mkdir master
cd prometheus && git checkout master
cp -v prompb/*.proto ../master
cd ..

rm -fr v2.5.0
mkdir v2.5.0
cd prometheus && git checkout v2.5.0
cp -v prompb/*.proto ../v2.5.0
cd ..

rm -fr prometheus
