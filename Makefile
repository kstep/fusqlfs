DEBUG="--logfile=fusqlfs.log"
DIR=`pwd`
L=100
M=5000

MDIR=$(DIR)/mnt
TABLEDIR=$(MDIR)/tables/testtable1
STRUCTDIR=$(TABLEDIR)/struct

remount: umount mount

umount:
	fusermount -u -z $(DIR)/mnt

mount:
	fusqlfs.pl $(DEBUG) -u postgres -e PgSQL -L $L unite_dev $(DIR)/mnt

clean-cache:
	pkill -USR1 fusqlfs.pl

test: test-syntax test-fs

test-syntax:
	@echo Syntax is correct
	find . -xdev -name "*.pl" -or -name "*.pm" -exec perl -c {} \;

test-lint:
	@echo Extended lint test
	find . -xdev -name "*.pl" -or -name "*.pm" -exec perl -MO=Lint,no-context {} \;

test-fs: test-basic test-tables

test-basic:
	@echo Common struct is sane
	test -d $(MDIR)/tables
	test -d $(MDIR)/views
	test -d $(MDIR)/roles
	test -d $(MDIR)/sequences

test-tables: test-tables-ls test-tables-create test-tables-struct test-tables-indices test-tables-data test-tables-drop

test-tables-ls:
	@echo Tables listing
	test -d $(DIR)/mnt/tables
	ls $(DIR)/mnt/tables/

test-tables-create:
	@echo Table create
	mkdir $(TABLEDIR)
	test -d $(TABLEDIR)
	test -d $(TABLEDIR)/struct 
	test -d $(TABLEDIR)/indices 
	test -d $(TABLEDIR)/data
	test -h $(TABLEDIR)/owner

test-tables-struct:
	@echo Table struct is sane
	test -d $(STRUCTDIR) -a -f $(STRUCTDIR)/id
	grep -q "^type: integer" $(STRUCTDIR)/id
	@echo Field rename
	mv $(STRUCTDIR)/id $(STRUCTDIR)/notid
	test ! -f $(STRUCTDIR)/id -a -f $(STRUCTDIR)/notid
	@echo Field create and change
	sed -ibak -e 's/type: integer/type: numeric(10,0)/' $(STRUCTDIR)/notid
	test -f $(STRUCTDIR)/notid -a -f $(STRUCTDIR)/notidbak
	grep -q "^type: integer" $(STRUCTDIR)/notidbak
	grep -q "^type: numeric(10,0)" $(STRUCTDIR)/notid
	@echo Field field drop
	rm -f $(STRUCTDIR)/notidbak
	test ! -f $(STRUCTDIR)/notidbak

test-tables-indices:
	echo TODO

test-tables-data:
	echo TODO

test-tables-drop:
	rmdir $(TABLEDIR)
	test ! -d $(TABLEDIR)

.PHONY: mount unmount remount \
	test test-fs test-basic \
	test-tables test-tables-ls test-tables-create test-tables-struct \
	test-tables-indices test-tables-data test-tables-drop

