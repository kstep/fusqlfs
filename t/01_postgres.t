use strict;
use v5.10.0;
use Test::More;
#plan tests => 5;
plan 'no_plan';

# Reqiured modules
use DBI;
use File::Temp qw(tempdir);
use POSIX qw(:fcntl_h :errno_h);
use Fcntl qw(:mode);

# Environment preparation
my $dbh = DBI->connect('DBI:Pg:database=postgres', 'postgres', '');
BAIL_OUT 'Unable to connect PostgreSQL: '.$DBI::errstr unless $dbh;

$dbh->do('DROP DATABASE IF EXISTS fusqlfs_test');
$dbh->do('CREATE DATABASE fusqlfs_test') or BAIL_OUT 'Unable to create test database: '.$dbh->errstr;
$dbh->disconnect;

#our $mount_dir = tempdir();
#BAIL_OUT 'Unable to create temporary mount point' unless $mount_dir;

# Mount test
require_ok 'FusqlFS';

eval {
	FusqlFS::init(
		engine   => 'PgSQL',
		user     => 'postgres',
		database => 'fusqlfs_test',
	);
};
ok !$@, 'FusqlFS initialization';

# Tables test
my @tables = qw(testtable);

is_deeply [ FusqlFS::getdir('/tables') ], [ '.', '..', 0 ], 'Tables module is on';
foreach (@tables)
{
	is FusqlFS::mkdir("/tables/$_"), 0, 'Table create';
	cmp_ok FusqlFS::getattr("/tables/$_"), '>=', 0, 'Table exists';
	is_deeply [ sort(FusqlFS::getdir("/tables/$_")) ],
			[ sort('.', '..', 'indices', 'struct', 'data', 'constraints', 'owner', 0) ], 'Table subdirectories are in place';

	# Indices
	is_deeply [ sort(FusqlFS::getdir("/tables/$_/indices")) ],
			[ sort('.', '..', "${_}_pkey", 0) ], 'Indices list';
	is_deeply [ sort(FusqlFS::getdir("/tables/$_/indices/${_}_pkey")) ],
			[ sort('.', '..', 'create.sql', 'id', '.order', '.primary', '.unique', 0) ], 'Primary key is sane';

	is FusqlFS::mkdir("/tables/$_/indices/myindex"), 0, 'Index create';
	cmp_ok FusqlFS::getattr("/tables/$_/indices/myindex"), '>=', 0, 'New index exists';
	is_deeply [ sort(FusqlFS::getdir("/tables/$_/indices/myindex")) ],
			[ sort('.', '..', '.order', 0) ], 'New index is empty';

	is FusqlFS::symlink("../../struct/id", "/tables/$_/indices/myindex/id"), 0, 'Index given field';
	is FusqlFS::readlink("/tables/$_/indices/myindex/id"), "../../struct/id", 'Index contains given field';
	cmp_ok FusqlFS::getattr("/tables/$_/indices/myindex"), '>=', 0, 'New index still exists';
	is_deeply [ sort(FusqlFS::getdir("/tables/$_/indices/myindex")) ],
			[ sort('.', '..', 'create.sql', 'id', '.order', 0) ], 'New index is sane';
	
	is FusqlFS::rmdir("/tables/$_/indices/myindex"), 0, 'Index remove';
	is FusqlFS::getattr("/tables/$_/indices/myindex"), -ENOENT(), 'Index doesn\'t exist via getattr';
	is FusqlFS::getdir("/tables/$_/indices/myindex"), -ENOENT(), 'Index doesn\'t exist via getdir';
	is FusqlFS::readlink("/tables/$_/indices/myindex/id"), -ENOENT(), 'Index field is gone';

	is FusqlFS::rmdir("/tables/$_"), 0, 'Table remove';
	is FusqlFS::getattr("/tables/$_"), -ENOENT(), 'Table doesn\'t exist via getattr';
	is FusqlFS::getdir("/tables/$_"), -ENOENT(), 'Table doesn\'t exist via getdir';
}


