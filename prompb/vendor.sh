#!/bin/bash

set -ex

rm -fr prom
git clone https://github.com/prometheus/prometheus.git prom

rm -fr prom-master
mkdir prom-master
cd prom && git checkout master
cp -v prompb/*.proto ../prom-master
cd ..

rm -fr prom1
mkdir prom1
cd prom && git checkout v1.8.2
cp -v storage/remote/*.proto ../prom1
cd ..

rm -fr prom2
mkdir prom2
cd prom && git checkout v2.3.2
cp -v prompb/*.proto ../prom2
cd ..

rm -fr prom
