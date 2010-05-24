
PERL ?= /usr/bin/perl

INSTALL_OPTS = --installdirs vendor $(if $(DESTDIR),--destdir $(DESTDIR),)

all: build

Build: Build.PL
	$(PERL) $<

manifest build: Build
	./Build $@

buildtests: build
	./Build $@

test testcover: buildtests testlint
	./Build $@

testlint:
	find ./lib -name "*.pm" -exec perl -M'lib "./lib"' -MO=Lint,no-context {} \;
	find ./bin -name "*.pl" -exec perl -M'lib "./lib"' -MO=Lint,no-context {} \;
	find ./t -name "*.t" -exec perl -M'lib "./lib"' -MO=Lint,no-context {} \;

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

testsclean:
	rm -f t/fusqlfs*.t t/manifest

cleanall: realclean debianclean testsclean

.PHONY: all manifest build test install debian dist \
	clean distclean realclean debianclean cleanall

