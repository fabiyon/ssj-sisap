CC=g++
CFLAGS=-c -w -Wall -D_LINUX -std=c++11
LDFLAGS=-lpthread
LDADD=-lgflags -lglog -lnuma -lpthread
SOURCES= io.cpp index.cpp tools.cpp algos.cpp mssj.cpp
OBJECTS=$(SOURCES:.cpp=.o)

all: CFLAGS += -O3
all: mssj

debug: CFLAGS += -g -O0
debug: mssj

mssj: $(OBJECTS)
	$(CC) $(LDFLAGS) $(OBJECTS) -o mssj $(LDADD)

.cpp.o:
	$(CC) $(CFLAGS) $< -o $@

.cc.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -rf *.o
	rm -rf mssj
