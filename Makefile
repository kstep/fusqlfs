
PERL ?= /usr/bin/perl

INSTALL_OPTS = --installdirs vendor $(if $(DESTDIR),--destdir $(DESTDIR),)

all: build

tags: lib
	ctags --language-force=Perl -R lib

mount: umount
	fusqlfs -e PgSQL -u postgres -l ./fusqlfs.log -L 100 -d unite_dev $(if $(MOP),$(MOP),-D) ./mnt

umount:
	-fusermount -u -z ./mnt

changelog:
	git tag -l | head -n $$(( `git tag -l | wc -l` - `git tag -l --contains HEAD | wc -l` + 1 )) | perl -ne 'next unless $$. == 1 or /^v[0-9.]+$$/; chomp; if ($$x) { print "\nChanged in $$_:\n\n"; print `git shortlog $$x..$$_`; }; $$x = $$_;' > Changelog

README.pod: bin/fusqlfs
	podselect ./bin/fusqlfs > README.pod

Build: Build.PL
	$(PERL) $<

manifest build: Build
	./Build $@

buildtests: build
	./Build $@

test testcover:
	./Build $@ $(if $T,--test_files $T,)

fulltest: buildtests testlint test

fullcover: buildtests testlint testcover

testlint:
	find ./lib -name "*.pm" -exec perl -M'lib "./lib"' -MO=Lint,no-context {} \;
	find ./bin -name "*.pl" -exec perl -M'lib "./lib"' -MO=Lint,no-context {} \;
	find ./t -name "*.t" -exec perl -M'lib "./lib"' -MO=Lint,no-context {} \;

install: Build
	./Build $@ $(INSTALL_OPTS)

debian: dist
	./Build $@

dist: cleanall README.pod changelog buildtests test manifest
	./Build $@

cpan: dist
	cpan-upload FusqlFS-*.tar.gz

clean:
	test ! -e Build || ./Build $@

realclean distclean:
	test ! -e Build || ./Build $@
	rm -f MYMETA.yml META.yml MANIFEST Changelog

debianclean:
	rm -rf debian

testsclean:
	rm -f t/fusqlfs*.t t/manifest

cleanall: realclean debianclean testsclean

.PHONY: all manifest build test install debian dist \
	clean distclean realclean debianclean cleanall \
	mount umount remount changelog fulltest fullcover \
	testcover buildtests cpan

