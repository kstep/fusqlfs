package MysqlFS;

use strict;

use YAML::Tiny;
use POSIX qw(:fcntl_h :errno_h mktime);
use Fcntl qw(:mode);
use DBI;

use Data::Dump qw(dump ddx);

require Exporter;
our (@ISA, @EXPORT);

@ISA = qw(Exporter);

BEGIN {
	@EXPORT = qw( 
		mysqlfs_initialize
		mysqlfs_getdir
		mysqlfs_getattr
		mysqlfs_mkdir
		mysqlfs_rmdir
		mysqlfs_symlink
		mysqlfs_readlink
		mysqlfs_unlink
		mysqlfs_rename
		mysqlfs_chmod
		mysqlfs_open
		mysqlfs_read
		mysqlfs_mknod
		mysqlfs_truncate
		mysqlfs_write
		mysqlfs_flush
		mysqlfs_release
		mysqlfs_utime
	);
}

our $dbh;

our %queries;
our %new_indexes;

our %base_rtxtsz;
our %base_itxtsz;
our $base_stxtsz;
our $def_base_itxtsz;

our %table_info_cache;
our %index_info_cache;

our $def_time;
our $def_charset;
our $def_engine;
our $fn_sep;

# host port database user password
sub mysqlfs_initialize {
	my %options = @_;

	my $dsn = "DBI:mysql:database=$options{database}";
	$dsn .= ";host=$options{host}" if ($options{'host'});
	$dsn .= ";port=$options{port}" if ($options{'port'});
	$dbh = DBI->connect($dsn, $options{'user'}, $options{'password'});

	if ($options{'charset'}) {
		$def_charset = $options{'charset'};
		$dbh->do("SET character_set_results = $def_charset");
		$dbh->do("SET character_set_client = $def_charset");
		$dbh->do("SET character_set_connection = $def_charset");
	}

	$def_engine = $options{'useinnodb'}? 'InnoDB': 'MyISAM'; 

	%queries = ();
	%new_indexes = ();

	%table_info_cache = ();
	%index_info_cache = ();

	%base_rtxtsz = ();
	%base_itxtsz = qw(decimal 95 set 63 enum 63 float 46 text 46);
	$base_stxtsz = 234;
	$def_base_itxtsz = 55;
	$fn_sep = $options{'fnsep'} || '.';

	$def_time = mktime(localtime());

	#$YAML::Syck::Headless = 1;
	#$YAML::Syck::SingleQuote = 1;
	#$YAML::UseHeader = 0;
} 

sub DESTROY {
	$dbh->disconnect();
	unlink $_ foreach glob("/tmp/".lc(__PACKAGE__)."/*.cache");
}

sub mysqlfs_chmod {
	my $file = shift;
	my $mode = shift;
	my @path = split /\//, $file;

	return -EACCES() unless $#path == 3 && $path[2] eq 'indeces';
	unless (exists $new_indexes{$path[1]}->{$path[3]}) {
		my $indexinfo = get_index_info($path[1], $path[3]);
		return -ENOENT() unless $indexinfo;
		my $unique = $mode & S_ISVTX;

		if ($unique != $indexinfo->{'Unique'}) {
			$indexinfo->{'Unique'} = $unique;
			drop_index($path[1], $path[3]);
			create_index($path[1], $path[3], $indexinfo);
		}
	} else {
		$new_indexes{$path[1]}->{$path[3]} = $mode;
	}

	return 0;
}

sub mysqlfs_utime {
	my ($file, $atime, $mtime) = @_;
	my @path = split /\//, $file;
	return -EACCES() unless $#path == 4 && $path[2] eq 'indeces';
	my $indexinfo = get_index_info($path[1], $path[3]);
	return -ENOENT() unless $indexinfo;
	my $tablestat = get_table_stat($path[1]);
	my $i = $#{ $indexinfo->{'Column_name'} };
	my %timestamps = map { $_ => $tablestat->{'Update_time'} + 86400 * $i-- } @{ $indexinfo->{'Column_name'} };
	$timestamps{$path[4]} = $mtime;
	$indexinfo->{'Column_name'} = [ sort { $timestamps{$b} <=> $timestamps{$a} } keys %timestamps ];
	drop_index($path[1], $path[3]);
	create_index($path[1], $path[3], $indexinfo);
	delete $index_info_cache{$path[1]}->{$path[3]};
	return 0;
}

sub mysqlfs_flush {
	my $file = shift;
	my @path = split /\//, $file;
	if ($#path > 1 && $path[1] ne '.queries') {
		delete $table_info_cache{$path[1]};
		delete $index_info_cache{$path[1]};
	}
	return 0;
}

# get information
sub mysqlfs_getattr {
	my $file = shift;

	my @fileinfo = (
		undef,	# 0 dev
		undef,	# 1 ino
		0644,	# 2 mode
		1,		# 3 nlink
		$>,		# 4 uid
		$),		# 5 gid
		undef,	# 6 rdev
		0,		# 7 size
		$def_time,	# 8 atime
		$def_time,	# 9 mtime
		$def_time,	# 10 ctime
		512,	# 11 blksize
		undef,	# 12 blocks
	);

	if ($file eq '/') {
		set_dir_info(\@fileinfo, 0);
	} elsif ($file eq '/.queries') {
		set_dir_info(\@fileinfo, scalar keys %queries);
	} elsif ($file eq '/.query') {
		set_file_info(\@fileinfo, -s get_cache_file_by_path([ '', '.query' ]));
		$fileinfo[2] &= ~0444;
	} else {
		my @path = split /\//, $file;
		my $tablestat;

		unless ($path[1] eq '.queries') {
			$tablestat = get_table_stat($path[1]);
			return -ENOENT() unless $tablestat;
			$fileinfo[8] = $tablestat->{'Check_time'};
			$fileinfo[9] = $tablestat->{'Update_time'};
			$fileinfo[10] = $tablestat->{'Create_time'};
		}

		if ($#path == 1) { # tables
			set_dir_info(\@fileinfo, 3);
		} elsif ($#path == 2) { # special dirs
			if ($path[1] eq '.queries') {
				return -ENOENT() unless exists $queries{$path[2]};
				my $cachefile = get_cache_file_by_path(\@path);
				unless (-e $cachefile) {
					delete $queries{$path[2]};
					return -ENOENT();
				}
				set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path));
				$fileinfo[2] &= ~0222;
			} else {
				if ($path[2] eq 'indeces'
					|| $path[2] eq 'struct'
					|| $path[2] eq 'data')
				{
					set_dir_info(\@fileinfo);
				} elsif ($path[2] eq 'status') {
					set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path) || $base_stxtsz + get_real_size($tablestat));
					$fileinfo[2] &= ~0222;
				} elsif ($path[2] eq 'create') {
					set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path) || length get_create_table($path[1]));
					$fileinfo[2] &= ~0222;
				} else {
					return -ENOENT();
				}
			}
		} elsif ($#path == 3) { # dir-indexes, records, fields
			if ($path[2] eq 'data') {
				my $record = get_record_by_file_name(\@path, 1);
				return -ENOENT() unless $record;# && %$record;
				set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path) || $base_rtxtsz{$path[1]} + get_real_size($record));
			} elsif ($path[2] eq 'struct') {
				my $tableinfo = get_table_info($path[1], $path[3]);
				return -ENOENT() unless $tableinfo;
				set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path) || ($base_itxtsz{$tableinfo->{'Type'}}||$def_base_itxtsz) + get_real_size($tableinfo));
			} elsif ($path[2] eq 'indeces') {
				unless (exists $new_indexes{$path[1]}->{$path[3]}) {
					my $indexinfo = get_index_info($path[1], $path[3]);
					return -ENOENT() unless $indexinfo;
					set_dir_info(\@fileinfo, scalar @{ $indexinfo->{'Column_name'} });
					$fileinfo[2] |= S_ISVTX if $indexinfo->{'Unique'};
				} else {
					$fileinfo[2] = $new_indexes{$path[1]}->{$path[3]};
					set_dir_info(\@fileinfo, 0);
				}
			}
		} elsif ($#path == 4 && $path[2] eq 'indeces') { # field info
			my @indexinfo = get_index_info($path[1], $path[3]);
			my $i = $#indexinfo; foreach (@indexinfo) { last if $_ eq $path[4]; $i--; }
			return -ENOENT() if $i < 0;
			$fileinfo[2] |= S_IFLNK;
			$fileinfo[7] = 21 + length($path[4]) - index($path[4], $fn_sep);
			$fileinfo[9] += 86400 * $i
		}
	}

	return @fileinfo;
}

sub mysqlfs_getdir {
	my $dir = shift;

	my @dir_list;

	if ($dir eq '/') {
		@dir_list = ('.query', '.queries', get_table_list());
	} elsif ($dir eq '/queries') {
		@dir_list = keys %queries;
	} else {
		my @path = split /\//, $dir;
		if ($#path == 1) { # spec dirs list
			@dir_list = ('create', 'status', 'struct', 'data', 'indeces');
		} else {
			if ($#path == 2) { # list of indexes/records/fields
				if ($path[2] eq 'data') {
					@dir_list = get_table_data($path[1]);
				} elsif ($path[2] eq 'struct') {
					@dir_list = get_table_info($path[1]);
				} elsif ($path[2] eq 'indeces') {
					@dir_list = ( get_index_info($path[1]), keys %{ $new_indexes{$path[1]} } );
				} else {
					return -ENOENT();
				}
			} elsif ($#path == 3) { # list of key fields
				return -ENOENT() unless $path[2] eq 'indeces';
				@dir_list = get_index_info($path[1], $path[3]);
			}
		}
	}

	return ('.', '..', @dir_list, 0);
}

# directories
sub mysqlfs_mkdir {
	my $dir = shift;
	my $mode = shift;
	my @path = split /\//, $dir;
	if ($#path == 1) { # create table
		return -EEXIST() if $path[1] eq '.queries';
		my @tableinfo = get_table_info($path[1]);
		return -EEXIST() if @tableinfo;
		create_table($path[1], 'id');
	} elsif ($#path == 3 && $path[2] eq 'indeces') { # create index
		return -EEXIST() if exists $new_indexes{$path[1]}->{$path[3]};
		my @indexinfo = get_index_info($path[1], $path[3]);
		return -EEXIST() if @indexinfo;
		$new_indexes{$path[1]}->{$path[3]} = $mode;
	} else {
		return -EACCES();
	}
	return 0;
}

sub mysqlfs_mknod {
	my ($file, $mode) = @_;
	my @path = split /\//, $file;

	if ($#path == 3) {
		if ($path[2] eq 'struct') {
			my @tableinfo = get_table_info($path[1], $path[3]);
			return -EEXIST() if @tableinfo;
			#return -EINVAL() unless defined 
			create_field($path[1], $path[3]);
			delete $table_info_cache{$path[1]};
		} elsif ($path[2] eq 'data') {
			my @record = get_record_by_file_name(\@path, 0);
			return -EEXIST() if @record;
			create_record($path[1], $path[3]);
		}
	} elsif ($#path == 2 && $path[1] eq '.queries') {
		return -EEXIST() if exists $queries{$path[2]};
		#return -EINVAL() unless $mode & S_IFIFO;
		return -EINVAL() unless $path[2] =~ /^(SELECT|SHOW)/i;

		my $sth = $dbh->prepare($path[2]);
		return -EINVAL() unless ($sth->execute());

		my $buffer = $sth->fetchall_arrayref({});
		if ($buffer && @$buffer) {
			$queries{$path[2]} = mktime(localtime()); 
			my $cachefile = get_cache_file_by_path(\@path);
			put_cache($cachefile, YAML::Tiny::Dump($buffer));
		}
		$sth->finish();
	} else {
		return -EACCES();
	}

	return 0;
}

# file open/read/write
sub mysqlfs_open {
	my ($file, $flags) = @_;
	my @path = split /\//, $file;

	return -EACCES() unless
			($flags > 0 && $file eq '/query')
			||
			($#path == 3 && ($path[2] eq 'struct' || $path[2] eq 'data'))
			||
			($#path == 2 && ($path[1] eq '.queries' || $path[2] eq '.status' || $path[2] eq '.create'));

	return 0;
} 

sub mysqlfs_read {
	my ($file, $size, $offset) = @_;
	my @path = split /\//, $file;

	my $buffer = '';
	my $cachefile = get_cache_file_by_path(\@path);

	if ($#path == 2 && $path[1] eq '.queries') {
		return -ENOENT() unless exists $queries{$path[2]};
		$buffer = get_cache($cachefile, $size, $offset) if -s $cachefile;
		return $buffer;
	} else {

		if (-r $cachefile) {
			$buffer = get_cache($cachefile);
		} else {
			if ($path[2] eq 'struct') {
				my $tableinfo = get_table_info($path[1], $path[3]);
				return -ENOENT() unless $tableinfo;
				$buffer = YAML::Tiny::Dump($tableinfo);
			} elsif ($path[2] eq 'data') {
				my $record = get_record_by_file_name(\@path, 1);
				return -ENOENT() unless $record;
				$buffer = YAML::Tiny::Dump($record);
			} elsif ($path[2] eq 'status') {
				my $tablestatus = get_table_stat($path[1]);
				return -ENOENT() unless $tablestatus;
				$buffer = YAML::Tiny::Dump($tablestatus);
			} elsif ($path[2] eq 'create') {
				my $createstatement = get_create_table($path[1]);
				return -ENOENT() unless $createstatement;
				$buffer = $createstatement;
			} else {
				return -EACCES();
			}
			put_cache($cachefile, $buffer) if $buffer;
		}
		$buffer .= " " x ($size - length $buffer) if $size > length $buffer;
		return substr($buffer, $offset, $size);
	}

}

sub mysqlfs_readlink {
	my $file = shift;
	my @path = split /\//, $file;
	return -ENOENT() unless $#path == 4 && $path[2] eq 'indeces';
	my ($name) = split /$fn_sep/, $path[4], 2;
	return "../../struct/$name";
}

sub mysqlfs_release {
	my ($file, $flags) = @_;

	my @path = split /\//, $file;
	my $cachefile = get_cache_file_by_path(\@path);

	if ($flags && -r $cachefile) {
		my $data;
		my $buffer = get_cache($cachefile);

		if ($file eq '/query') {
			#my @statements = map { s/^--.*//gm; s/\/\*.*\*\///gm; s/^\s+//; s/\s+$//gm; s/^;$//gm; } split /;\n/, $buffer;
			foreach (split /;\n/, $buffer) {
				s/^\s+//; s/\s+$//;
				next unless $_;
				$dbh->do($_);
			}
		} else {

# YAML::Tiny produces list, not hashref, Load will panic on wrong data
			$data = YAML::Tiny->read_string($buffer) if length $buffer > 3;

			if ($data) {
				undef $buffer;
				if ($path[2] eq 'struct') {
					modify_field($path[1], $path[3], $data->[0])
				} elsif ($path[2] eq 'data') {
					save_record($path[1], parse_file_name_to_record($path[1], $path[3]), $data->[0]);
				} else {
					return -EACCES();
				}
			}# else {
			#	return -EINVAL();
			#}
		}
	}

	unlink $cachefile unless $path[1] eq '.queries';
	return 0;
}

sub mysqlfs_rename {
	my ($file, $nfile) = @_;
	my @path = split /\//, $file;
	my @npath = split /\//, $nfile;

	return -EACCES() unless $#path == $#npath;
	return -EACCES() if $path[1] eq '.queries';

	return 0 if $path[$#path] eq $npath[$#npath];

	if ($#path == 1) { # rename table
		my @tableinfo = get_table_info($path[1]);
		return -ENOENT() unless @tableinfo;
		rename_table($path[1], $npath[1]);
	} else {
		return -EACCES() unless $path[1] eq $npath[1];
		if ($#path == 3) { # rename field, index or record
			if ($path[2] eq 'struct') {
				my $tableinfo = get_table_info($path[1], $path[3]);
				return -ENOENT() unless $tableinfo;
				change_field($path[1], $path[3], $npath[3]);
			} elsif ($path[2] eq 'data') {

				my $record = get_record_by_file_name(\@path, 0);
				return -ENOENT() unless $record;

				my @nvalues = split /$fn_sep/, $npath[3];
				return -EINVAL() unless scalar @nvalues == scalar keys %$record;

				my $i = 0;
				my %nrecord = map { $_ => $nvalues[$i++] } sort keys %$record;
				update_record($path[1], $record, \%nrecord);

			} elsif ($path[2] eq 'indeces') {
				my $indexinfo = get_index_info($path[1], $path[3]);
				return -ENOENT() unless $indexinfo;
				drop_index($path[1], $path[3]);
				create_index($path[1], $npath[3], $indexinfo);
			}
		} elsif ($#path == 4 && $path[2] eq 'indeces') { # change field in index
			my $indexinfo = get_index_info($path[1], $path[3]);
			return -ENOENT() unless $indexinfo;
			@{ $indexinfo->{'Column_name'} } = map { $_ eq $path[4]? $npath[4]: $_ } @{ $indexinfo->{'Column_name'} };
			drop_index($path[1], $path[3]);
			create_index($path[1], $path[3], $indexinfo);
		} else {
			return -EACCES();
		}
 	}
	return 0;
}

sub mysqlfs_rmdir {
	my $dir = shift;
	my @path = split /\//, $dir;
	if ($#path == 1) { # drop table
		return -EACCES() if $path[1] eq '.queries';
		drop_table($path[1]);
		delete $table_info_cache{$path[1]};
	} elsif ($#path == 3) { # drop index
		if (exists $new_indexes{$path[1]}->{$path[3]}) {
			delete $new_indexes{$path[1]}->{$path[3]};
		} else {
			drop_index($path[1], $path[3]);
			delete $index_info_cache{$path[1]}->{$path[3]};
		}
	} else {
		return -EACCES();
	}
	return 0;
}

# symbolic links
sub mysqlfs_symlink {
	my ($file, $link) = @_;
	my @path = split /\//, $file;
	my @lpath = split /\//, $link;

	unshift @path, @lpath[0..$#lpath-1];
	for (my $i = 0; $i <= $#lpath; $i++) {
		if ($path[$i] eq '..') {
			splice @path, $i-1, 2;
			$i -= 2;
		}
	}

	return -EINVAL() unless $#path == 3 && $#lpath == 4
			&& $path[2] eq 'struct' && $lpath[2] eq 'indeces'
			&& $path[1] eq $lpath[1];

	my @name = split /$fn_sep/, $lpath[4];
	return -EINVAL() unless $#name < 2 && $name[0] eq $path[3];

	my $indexinfo;
	if (exists $new_indexes{$lpath[1]}->{$lpath[3]}) {
		$indexinfo = { 'Unique' => $new_indexes{$lpath[1]}->{$lpath[3]} & S_ISVTX, 'Column_name' => [ $lpath[4] ] };
		delete $new_indexes{$lpath[1]}->{$lpath[3]};
	} else {
		$indexinfo = get_index_info($lpath[1], $lpath[3]);
		push @{ $indexinfo->{'Column_name'} }, $lpath[4];
		drop_index($lpath[1], $lpath[3]);
	}
	create_index($lpath[1], $lpath[3], $indexinfo);
	return 0;
}

sub mysqlfs_truncate {
	my $file = shift;

	my @path = split /\//, $file;
	return -EACCES() unless
					($file eq '/queries')
					||
					($#path == 3 && ($path[2] eq 'data' || $path[2] eq 'struct'));

	delete $table_info_cache{$path[1]};
	delete $index_info_cache{$path[1]};
	#unlink get_cache_file_by_path(\@path);

	return 0;
}

sub mysqlfs_unlink {
	my $file = shift;
	return 0 if $file eq '/query';
	my @path = split /\//, $file;
	if ($#path == 4 && $path[2] eq 'indeces') {
		my $indexinfo = get_index_info($path[1], $path[3]);
		my $ok = 0;
		return -ENOENT() unless $indexinfo;
		for (my $i = 0; $i <= $#{ $indexinfo->{'Column_name'} }; $i++) {
			if ($indexinfo->{'Column_name'}->[$i] eq $path[4]) {
				splice @{ $indexinfo->{'Column_name'} }, $i, 1;
				$ok = 1;
				last;
			}
		}
		return -ENOENT() unless $ok;
		drop_index($path[1], $path[3]);
		create_index($path[1], $path[3], $indexinfo);
		delete $index_info_cache{$path[1]}->{$path[3]};
	} elsif ($#path == 3) {
		if ($path[2] eq 'data') {
			my $condition = parse_file_name_to_record($path[1], $path[3]);
			return -EINVAL() unless $condition;
			delete_record($path[1], $condition);
		} elsif ($path[2] eq 'struct') {
			drop_field($path[1], $path[3]);
			delete $table_info_cache{$path[1]};
			delete $index_info_cache{$path[1]};
		} else {
			return -EACCES();
		}
	} elsif ($#path == 2 && $path[1] eq '.queries') {
		return -ENOENT() unless exists $queries{$path[2]};
		unlink get_cache_file_by_path(\@path);
		delete $queries{$path[2]};
	} else {
		return -EACCES();
	}
	return 0;
}

sub mysqlfs_write {
	my ($file, $buffer, $offset) = @_;

	my @path = split /\//, $file;
	return -EACCES() unless ($file eq '/query') || ($#path == 3 && ($path[2] eq 'struct' || $path[2] eq 'data'));
	my $cachefile = get_cache_file_by_path(\@path);

	put_cache($cachefile, $buffer, $offset);

	return length $buffer;
}
sub delete_record {
	my ($table, $record) = @_;
	my $sql = "DELETE FROM $table WHERE ";
	$sql .= join(' AND ', map { "$_ = ?" } keys %$record);
	return $dbh->do($sql, undef, values %$record);
}
sub get_table_info {
	my ($table, $field) = @_;

	if (exists $table_info_cache{$table} && %{ $table_info_cache{$table} }) {

		if (wantarray) {
			return $field? exists $table_info_cache{$table}->{$field}?($field):(): keys %{ $table_info_cache{$table} };
		} else {
			return $field? $table_info_cache{$table}->{$field}:
					$table_info_cache{$table};
		}
	}

	my $sth = $dbh->prepare("SHOW COLUMNS FROM $table");

	my %result;

	$sth->execute();
	$base_rtxtsz{$table} = 4;
	while (my @row = $sth->fetchrow_array()) {
		$base_rtxtsz{$table} += 3 + length($row[0]);
		next if $field && $row[0] ne $field;
		$result{$row[0]} = {
			'Not_null'	=> $row[2] eq 'NO' || 0, # 7
			'Key'		=> $row[3], # 6
			'Default'	=> $row[4], # 10
			'Extra'		=> $row[5]  # 8
		};
		my ($type, $info) = (split /\(/, $row[1], 2);
		$result{$row[0]}->{'Type'} = $type;
		if ($type eq 'decimal') {
			my ($length, $decimal) = split /,/, $info;
			$result{$row[0]}->{'Length'} = 0 + $length;
			$result{$row[0]}->{'Decimal'} = 0 + $decimal;
			$result{$row[0]}->{'Zerofill'} = $row[1] =~ /zerofill/ || 0;
			$result{$row[0]}->{'Unsigned'} = $row[1] =~ /unsigned/ || 0;
		} elsif ($type eq 'set' || $type eq 'enum') {
			$result{$row[0]}->{'Enum'} = [ map { s/''/'/g; $_ } split(/','/, substr($info, 1, -2)) ];
		} elsif ($info) {
			$result{$row[0]}->{'Length'} = 0 + $info;
		}
	}
	$sth->finish();

	$table_info_cache{$table} = \%result unless $field;
	return wantarray? keys %result: ($field? $result{$field}: \%result);
}
sub get_cache_file_by_path {
	my $path = shift;
	my $file = "/tmp/".lc(__PACKAGE__);
# e.g. /tmp/mysqlfs.not_auth.id..struct"
	if ($path->[1] eq '.query') {
		return "$file.query.cache";
	} elsif ($path->[1] eq '.queries') {
		return "$file.$queries{$path->[2]}.queries.cache";
	} else {
		return "$file.$path->[1].$path->[3].$path->[2]";
	}
}

# ALTER TABLE ... RENAME TO ...

# DROP INDEX ...

# SHOW TABLES
# in: void
#out: list of tables
sub get_table_list {
	my $result = $dbh->selectcol_arrayref("SHOW TABLES") || [];
	return @$result;
}
sub get_index_info {
	my ($table, $index) = @_;

	if (exists $index_info_cache{$table}->{$index}) {
		if (wantarray) {
			return $index? @{ $index_info_cache{$table}->{$index}->{'Column_name'} }: keys %{ $index_info_cache{$table} };
		} else {
			return $index? $index_info_cache{$table}->{$index}: $index_info_cache{$table};
		}
	}

	my %result;
	my $sth = $dbh->prepare("SHOW INDEX FROM $table");
	
	$sth->execute();
	while (my @row = $sth->fetchrow_array()) {
		next if $index && $row[2] ne $index;
		if (exists $result{$row[2]}) {
			push @{ $result{$row[2]}->{'Column_name'} }, $row[4].($row[7] && "$fn_sep$row[7]");
		} else {
			$result{$row[2]} = {
				'Unique'		=> !$row[1] || 0,
				'Column_name'	=> [ $row[4].($row[7] && "$fn_sep$row[7]") ],
				'Collation'		=> $row[5],
				'Cardinality'	=> 0 + $row[6],
				'Packed'		=> $row[8],
				'Not_null'		=> !$row[9] || 0,
				'Index_type'	=> $row[10],
				'Comment'		=> $row[11],
			};
		}
	}
	$sth->finish();

	$index_info_cache{$table} = \%result unless $index;

	return wantarray? ($index? @{ $result{$index}->{'Column_name'} || [] }: keys %result): ($index? $result{$index}: \%result);
}
sub create_table {
	return $dbh->do("CREATE TABLE $_[0] ($_[1] int NOT NULL auto_increment, PRIMARY KEY (id))".($def_engine && " ENGINE=$def_engine").($def_charset && " DEFAULT CHARSET=$def_charset"));
}
sub drop_table {
	return $dbh->do("DROP TABLE $_[0]");
}
sub parse_file_name_to_record {
	my ($table, $filename) = @_;
	my @keys = get_primary_key($table);
	my @values = split /$fn_sep/, $filename, scalar @keys;
	return undef unless $#values == $#keys;
	my $i = 0;
	my %result;
	%result = map { $_ => $values[$i++] } @keys;
	return \%result;
}
sub set_dir_info {
	my $fileinfo = shift;
	$fileinfo->[2] |= (S_IFDIR|0111);
	$fileinfo->[3] = 2 + shift;
}

# ALTER TABLE ... CHANGE ...

# UPDATE ... SET ... WHERE ... 
sub put_cache {
	my ($cachefile, $buffer, $offset) = @_;
	local $/;
	open FCACHE, '>>', $cachefile;
	seek FCACHE, $offset, 0 if $offset;
	print FCACHE $buffer;
	close FCACHE;
}


# create new default "empty" field
# (something line 'xxx int not null default 0')
sub create_record {
	my ($table, $name) = @_;
	my $tableinfo = get_table_info($table);
	my %record;

	unless ($name eq 'auto') {
		my @keys = grep $tableinfo->{$_}->{'Key'} eq 'PRI',
					sort keys %$tableinfo;
		my @values = split /$fn_sep/, $name;
		my $i = 0;
		%record = map { $_ => $values[$i++] } @keys;
	}

	while (my ($key, $field) = each %$tableinfo) {
		next unless $field->{'Not_null'} && $field->{'Default'} eq '';
		next if $field->{'Extra'} =~ /auto_increment/;

		if ($field->{'Type'} eq 'set' || $field->{'Type'} eq 'enum')
		{
			$record{$key} = $field->{'Enum'}->[0];
		} elsif ($field->{'Type'} eq 'float' || $field->{'Type'} eq 'decimal'
			|| $field->{'Type'} =~ /int/)
		{
			$record{$key} = 0;
		} else {
			$record{$key} = '';
		} 
	}

	return insert_record($table, \%record);

}
sub get_real_size {
	my $obj = shift;
	my @values = ref $obj eq 'ARRAY'? @$obj: values %$obj;
	my $size = 0;
	map { $size += ref $_? get_real_size($_): defined $_ ? (length($_) + 2): 1 } @values;
	return $size;
}

# create new empty record with
# all fields set to default values
sub save_record {
	my ($table, $condition, $record) = @_;
	my $crecord = get_record($table, $condition);
	if ($crecord) {
		update_record($table, $condition, $record);
	} else {
		insert_record($table, $record);
	}
}
sub rename_table {
	$dbh->do("ALTER TABLE $_[0] RENAME TO $_[1]");
}

# $condition, $full



# CREATE TABLE
sub get_record_by_file_name {
	my ($path, $full) = @_;
	my $condition = parse_file_name_to_record($path->[1], $path->[3]);
	return undef unless $condition;
	return wantarray? (get_record($path->[1], $condition, $full)): get_record($path->[1], $condition, $full);
}
sub parse_mysql_time {
	my $time = shift;
	my $result = 0;
	#              1 year  2 month 3 day   4 hour  5 min   6 sec
	if ($time =~ /(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})/) {
		$result = mktime($6, $5, $4, $3, $2 - 1, $1 - 1900);
	}
	return $result;
}

# insert or update rec,
# depending on its existance

# SHOW INDEX FROM ...
# in: $table, $index [opt]
#out: in scalar context:
#     if $index, hashref with this index desc,
#     else hashref with desc of indexes for table,
#     in array context:
#     if $index, list of fields, included in index,
#     else list of all indexes for table.


# SHOW COLUMNS FROM ...
# in: $table, $field [opt]
#out: in scalar context:
#     if $field is set, hashref with desc of the field,
#     else hashref with total desc of table struct,
#     in array context:
#     if $field is set, boolean if field is exist, 
#     else list of all fields in a table.
sub modify_field {
	my ($table, $field, $fdesc) = @_;
	my ($sql, @values) = convert_field_to_sql($fdesc);
	print STDERR "ALTER TABLE $table MODIFY $field $sql\n";
	return $dbh->do("ALTER TABLE $table MODIFY $field $sql", undef, @values);
}

sub convert_field_to_sql {
	my $fdesc = shift;
	return unless ref($fdesc) eq "HASH";
	my $sql = $fdesc->{'Type'};
	my @values;
	if ($fdesc->{'Type'} eq 'enum' || $fdesc->{'Type'} eq 'set') {
		@values = @{ $fdesc->{'Enum'} };
		$sql .= "(".substr(',?' x scalar @values, 1).")";
	} elsif ($fdesc->{'Type'} eq 'decimal') {
		$sql .= "($fdesc->{Length},$fdesc->{Decimal})";
		$sql .= " unsigned" if $fdesc->{'Unsigned'};
		$sql .= " zerofill" if $fdesc->{'Zerofill'};
	} elsif ($fdesc->{'Length'}) {
		$sql .= "($fdesc->{Length})"; 
	}
	$sql .= " NOT NULL" if $fdesc->{'Not_null'};
	if ($fdesc->{'Default'}) {
		$sql .= " DEFAULT ?";
		push @values, $fdesc->{'Default'};
	}
	$sql .= " $fdesc->{Extra}" if $fdesc->{'Extra'};
	return ($sql, @values);
}
sub create_index {
	my ($table, $index, $idesc) = @_;
	my @fields = @{ $idesc->{'Column_name'} };
	my $index = $index =~ /^PRI/? 'PRIMARY KEY': ($idesc->{'Unique'}? 'UNIQUE ':'')."KEY $index";
	my $sql = "ALTER TABLE $table ADD $index (";
	$sql .= join(',', map { my ($name, $part) = split /$fn_sep/, $_; $part += 0; $part? "$name($part)": $name } @fields); 
	$sql .= ")";
	return $dbh->do($sql);
}

# CREATE INDEX ...

sub update_record {
	my ($table, $condition, $record) = @_;
	my $sql = "UPDATE $table SET ";
	my @values = (values %$record, values %$condition);
	$sql .= join(',', map { "$_ = ?" } keys %$record);
	$sql .= " WHERE ". join(' AND ', map { "$_ = ?" } keys %$condition);
	return $dbh->do($sql, undef, @values);
}
sub insert_record {
	my ($table, $record) = @_;
	my $sql = "INSERT INTO $table (";
	$sql .= join(',', keys %$record);
	$sql .= ") VALUES (". substr(',?' x scalar keys %$record, 1) .")";
	return $dbh->do($sql, undef, values %$record);
}
sub set_file_info {
	my ($fileinfo, $size) = @_;
	$fileinfo->[2] |= S_IFREG;
	$fileinfo->[7] = 0 + $size;
	#$fileinfo->[12] = $size / $fileinfo->[11] if $fileinfo->[11];
}

# ALTER TABLE ... MODIFY ...

sub get_table_data {
	my $table = shift;
	my @keys = get_primary_key($table);
	return () unless @keys;
	my $result = $dbh->selectcol_arrayref("SELECT CONCAT_WS('$fn_sep',".join(',',@keys).") FROM $table") || [];
	return @$result;
}

sub change_field {
	my ($table, $field, $nfield, $fdesc) = @_;
	$fdesc ||= get_table_info($table, $field);
	my ($sql, @values) = convert_field_to_sql($fdesc);
	print STDERR "ALTER TABLE $table CHANGE $field $nfield $sql\n";
	return $dbh->do("ALTER TABLE $table CHANGE $field $nfield $sql", undef, @values);
}

# DELETE FROM ... WHERE ...

# SHOW TABLE STATUS [LIKE '...']
# in: $table [opt]
#out: hashref with table(s) status



sub create_field {
	return $dbh->do("ALTER TABLE $_[0] ADD $_[1] int NOT NULL DEFAULT 0");
}
sub drop_index {
	my ($table, $index) = @_;
	my $index = $index =~ /^PRI/? 'PRIMARY KEY': "KEY $index";
	return $dbh->do("ALTER TABLE $table DROP $index");
}
sub get_cache {
	my ($cachefile, $size, $offset) = @_;
	my $buffer;
	local $/;
	open FCACHE, $cachefile;
	if ($size) {
		seek FCACHE, $offset, 0 if $offset;
		read FCACHE, $buffer, $size;
	} else {
		$buffer = <FCACHE>;
	}
	close FCACHE;
	return $buffer;
}
sub get_table_stat {
	my $table = shift;

	my %result;
	my $sth = $dbh->prepare("SHOW TABLE STATUS".(defined $table && " LIKE '$table'"));

	$sth->execute();
	while (my @row = $sth->fetchrow_array()) {
		$result{$row[0]} = {
			'Engine'			=> $row[1],
			'Version'			=> $row[2],
			'Row_format'		=> $row[3],
			'Rows'				=> 0 + $row[4],
			'Avg_row_length'	=> 0 + $row[5],
			'Data_length'		=> 0 + $row[6],
			'Max_data_length'	=> 0 + $row[7],
			'Index_length'		=> 0 + $row[8],
			'Data_free'			=> 0 + $row[9],
			'Auto_increment'	=> 0 + $row[10],
			'Create_time'		=> parse_mysql_time($row[11]),
			'Update_time'		=> parse_mysql_time($row[12]),
			'Check_time'		=> parse_mysql_time($row[13]),
			'Collation'			=> $row[14],
			'Checksum'			=> $row[15],
			'Create_options'	=> $row[16],
			'Comment'			=> $row[17],
		};
	}
	$sth->finish();

	return $table? $result{$table}: \%result;
}

# INSERT INTO ... VALUES ...
sub get_primary_key {
	my $table = shift;
	my $tableinfo = get_table_info($table);
	return grep $tableinfo->{$_}->{'Key'} eq 'PRI', sort keys %$tableinfo;
}



# ALTER TABLE ... DROP ...
sub get_record {
	my ($table, $condition, $full) = @_;
	my @keys = keys %$condition; 
	my $sql = "SELECT ". ($full? "*": join(',', @keys));
	$sql .= " FROM $table WHERE ". join(' AND ', map { "$_ = ?" } @keys);
	return wantarray? $dbh->selectrow_array($sql, undef, values %$condition): $dbh->selectrow_hashref($sql, undef, values %$condition);
}
sub drop_field {
	return $dbh->do("ALTER TABLE $_[0] DROP $_[1]");
}

sub get_create_table {
	my @row = $dbh->selectrow_array("SHOW CREATE TABLE $_[0]");
	return @row? $row[1]: undef;
}

1;
