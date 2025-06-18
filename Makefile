SRC_DIR = src
BIN_DIR = bin
MAIN_CLASS = LoadBalancer.LoadBalancer  # update if needed

.PHONY: build run runf clean

build:
	mkdir -p $(BIN_DIR)
	find $(SRC_DIR) -name "*.java" > sources.txt
	javac -d $(BIN_DIR) @sources.txt
	rm sources.txt

run: build
	java -cp $(BIN_DIR) $(MAIN_CLASS)

runf: run

clean:
	rm -rf $(BIN_DIR)/*
