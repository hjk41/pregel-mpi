CCOMPILE=mpiicc
CFLAGS=
ARCHIVE=

all: ssp

ssp: ssp.cpp
	$(CCOMPILE) ssp.cpp -o ssp -I ../../lib $(CFLAGS) $(ARCHIVE) -Wno-deprecated

clean:
	-rm ssp
