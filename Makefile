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

test-syntax:
	find . -xdev -name "*.pl" -or -name "*.pm" -exec perl -c {} \;

test-lint:
	@echo Extended lint test
	find . -xdev -name "*.pl" -or -name "*.pm" -exec perl -MO=Lint,no-context {} \;

test-fs: test-basic test-tables

test-basic:
	test -d $(MDIR)/tables
	test -d $(MDIR)/views
	test -d $(MDIR)/roles
	test -d $(MDIR)/sequences

test-tables: test-tables-ls test-tables-create test-tables-struct test-tables-indices test-tables-data test-tables-drop

test-tables-ls:
	test -d $(DIR)/mnt/tables
	ls $(DIR)/mnt/tables/

test-tables-create:
	mkdir $(TABLEDIR)
	test -d $(TABLEDIR)
	test -d $(TABLEDIR)/struct 
	test -d $(TABLEDIR)/indices 
	test -d $(TABLEDIR)/data
	test -h $(TABLEDIR)/owner

test-tables-struct:
	test -d $(STRUCTDIR) -a -f $(STRUCTDIR)/id
	grep -q "^type: integer" $(STRUCTDIR)/id
	mv $(STRUCTDIR)/id $(STRUCTDIR)/notid
	test ! -f $(STRUCTDIR)/id -a -f $(STRUCTDIR)/notid
	sed -ibak -e 's/type: integer/type: numeric(10,0)/' $(STRUCTDIR)/notid
	test -f $(STRUCTDIR)/notid -a -f $(STRUCTDIR)/notidbak
	grep -q "^type: integer" $(STRUCTDIR)/notidbak
	grep -q "^type: numeric(10,0)" $(STRUCTDIR)/notid
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
	test-basic \
	test-tables test-tables-ls test-tables-create test-tables-struct \
	test-tables-indices test-tables-data test-tables-drop

