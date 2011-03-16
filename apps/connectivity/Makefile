CCOMPILE=mpiicc
CFLAGS=
ARCHIVE=

all: connectivity

connectivity: connectivity.cpp
	$(CCOMPILE) connectivity.cpp -o connectivity -I ../../lib $(CFLAGS) $(ARCHIVE) -Wno-deprecated

clean:
	-rm connectivity
