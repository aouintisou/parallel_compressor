CC      = gcc
CFLAGS  = -Wall -Wextra -O2 -Iinclude
LDFLAGS = -lpthread -lz

SRC     = src/main.c src/ipc.c src/worker.c
TARGET  = compressor

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET) $(LDFLAGS)

clean:
	rm -f $(TARGET)
	rm -rf output_files/ decompressed/
	rm -f test_files/*.gz

run:
	./compressor --processes 2 --threads 2 --mode compress

decompress:
	./compressor --processes 2 --threads 2 --mode decompress \
		--input output_files --output decompressed

benchmark:
	./compressor --benchmark --mode compress

.PHONY: all clean run decompress benchmark
