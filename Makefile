
PERL ?= /usr/bin/perl

INSTALL_OPTS = $(if $(DESTDIR),--installdirs vendor --destdir $(DESTDIR),)

all: build

Build: Build.PL
	$(PERL) $<

manifest build test: Build
	./Build $@

install: Build
	./Build $@ $(INSTALL_OPTS)

debian: dist
	./Build $@

dist: manifest
	./Build $@

clean:
	test ! -e Build || ./Build $@

realclean distclean:
	test ! -e Build || ./Build $@
	rm -f META.yml MANIFEST

debianclean:
	rm -rf debian

cleanall: realclean debianclean

.PHONY: all manifest build test install debian dist \
	clean distclean realclean debianclean cleanall

