#!/bin/bash

mkdir -p out

CP="lib/*:."

javac -cp "$CP" -d out $(find src -name "*.java") &&

java -cp "out:$CP" dev.bytekv.Main
