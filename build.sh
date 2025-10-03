#!/bin/bash

mkdir -p out

CP="lib/*:."

> ./logs/master.log

rm -rf ./SST/*
rm -rf ./SST/.* 2>/dev/null

javac -cp "$CP" -d out $(find src -name "*.java") &&

java -cp "out:$CP" dev.bytekv.BenchMark
