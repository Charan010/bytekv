SRC_DIR = src
BIN_DIR = $(SRC_DIR)/bin
MAIN_CLASS = Main

.PHONY: build run runf clean

build:
	mkdir -p $(BIN_DIR)
	find $(SRC_DIR) -name "*.java" > sources.txt
	javac -d $(BIN_DIR) @sources.txt
	rm sources.txt

run: build
	java -cp $(BIN_DIR) src.Main

runf: run

clean:
	rm -rf $(BIN_DIR)/*
