#!/bin/bash
mkdir -p out

javac -cp "lib/protobuf-java-3.25.3.jar:." \
  -d out \
  $(find src -name "*.java") &&

java -cp "out:lib/protobuf-java-3.25.3.jar" dev.bytekv.Main
