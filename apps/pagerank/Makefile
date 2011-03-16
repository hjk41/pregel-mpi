CCOMPILE=mpiicc
CFLAGS=
ARCHIVE=

all: pagerank

pagerank: pagerank.cpp
	$(CCOMPILE) pagerank.cpp -o pagerank -I ../../lib $(CFLAGS) $(ARCHIVE) -Wno-deprecated

clean:
	-rm pagerank
