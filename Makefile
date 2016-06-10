CC = gcc
CFLAGS = -Iinclude -fPIC -Wall -Wextra -lc -lm -std=gnu99 -g
LDFLAGS = -shared

CFLAGS += -O0
# CFLAGS += -02

TARGET = tdigest.so
SOURCES = $(wildcard src/*.c)
HEADERS = $(wildcard include/*.h)
OBJECTS = $(SOURCES:.c=.o)

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(LD) -o $@ $(LDFLAGS) -lm -lc $(OBJECTS)

clean:
	find . -type f -name '*.o' -delete
	find . -type f -name '*.so' -delete
