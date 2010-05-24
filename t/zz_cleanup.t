use strict;
use Test::More;
plan tests => 1;

use DBI;
my $dbh = DBI->connect('DBI:Pg:database=postgres', 'postgres');

eval {
$dbh->do('DROP DATABASE IF EXISTS fusqlfs_test');
$dbh->do('DROP ROLE fusqlfs_test');
$dbh->do('DROP ROLE new_fusqlfs_test');
};
undef $@;

ok 1, 'Database is clean';
