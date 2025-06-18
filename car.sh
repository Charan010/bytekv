#!/bin/bash

javac -cp "lib/*" -d bin $(find src -name "*.java")
java -cp "bin:lib/*" src.LoadBalancer.LoadBalancer
