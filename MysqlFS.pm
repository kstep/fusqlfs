package MysqlFS;

use strict;

use YAML::Tiny;
use POSIX qw(:fcntl_h :errno_h mktime);
use Fcntl qw(:mode);
use MySQL;

use Data::Dump qw(dump ddx);

require Exporter;
our (@ISA, @EXPORT);

@ISA = qw(Exporter);

BEGIN {
    @EXPORT = qw(
    initialize
    getdir
    getattr
    mkdir
    rmdir
    symlink
    readlink
    unlink
    rename
    chmod
    open
    read
    mknod
    truncate
    write
    flush
    release
    utime
    );
}

our %queries;
our %new_indexes;

our $def_time;
our $fn_sep;

# host port database user password
sub initialize {
    my %options = @_;

    MySQL::init_db(\%options);

    %queries = ();
    %new_indexes = ();

    $fn_sep = $options{'fnsep'} || '.';

    $def_time = mktime(localtime());

    #$YAML::Syck::Headless = 1;
    #$YAML::Syck::SingleQuote = 1;
    #$YAML::UseHeader = 0;
}

sub DESTROY {
    unlink $_ foreach glob("/tmp/".lc(__PACKAGE__)."/*.cache");
}

sub chmod {
    my $file = shift;
    my $mode = shift;
    my @path = split /\//, $file;

    return -EACCES() unless $#path == 3 && $path[2] eq 'indeces';
    unless (exists $new_indexes{$path[1]}->{$path[3]}) {
        my $indexinfo = MySQL::get_index_info($path[1], $path[3]);
        return -ENOENT() unless $indexinfo;
        my $unique = $mode & S_ISVTX;

        if ($unique != $indexinfo->{'Unique'}) {
            $indexinfo->{'Unique'} = $unique;
            MySQL::modify_index($path[1], $path[3], $indexinfo);
        }
    } else {
        $new_indexes{$path[1]}->{$path[3]} = $mode;
    }

    return 0;
}

sub utime {
    my ($file, $atime, $mtime) = @_;
    my @path = split /\//, $file;
    return -EACCES() unless $#path == 4 && $path[2] eq 'indeces';
    my $indexinfo = MySQL::get_index_info($path[1], $path[3]);
    return -ENOENT() unless $indexinfo;
    my $tablestat = MySQL::get_table_stat($path[1]);
    my $i = $#{ $indexinfo->{'Column_name'} };
    my %timestamps = map { $_ => $tablestat->{'Update_time'} + 86400 * $i-- } @{ $indexinfo->{'Column_name'} };
    $timestamps{$path[4]} = $mtime;
    $indexinfo->{'Column_name'} = [ sort { $timestamps{$b} <=> $timestamps{$a} } keys %timestamps ];
    MySQL::modify_index($path[1], $path[3], $indexinfo);
    return 0;
}

sub flush {
    my $file = shift;
    my @path = split /\//, $file;
    if ($#path > 1 && $path[1] ne '.queries') {
        MySQL::flush_table_cache($path[1]);
    }
    return 0;
}

# get information
sub getattr {
    my $file = shift;

    my @fileinfo = (
        undef,     # 0 dev
        undef,     # 1 ino
        0644,      # 2 mode
        1,         # 3 nlink
        $>,        # 4 uid
        $),        # 5 gid
        undef,     # 6 rdev
        0,         # 7 size
        $def_time, # 8 atime
        $def_time, # 9 mtime
        $def_time, # 10 ctime
        512,       # 11 blksize
        undef,     # 12 blocks
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
            $tablestat = MySQL::get_table_stat($path[1]);
            return -ENOENT() unless $tablestat;
            $fileinfo[8] = $tablestat->{'Check_time'} || $def_time;
            $fileinfo[9] = $tablestat->{'Update_time'} || $def_time;
            $fileinfo[10] = $tablestat->{'Create_time'} || $def_time;
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
                    set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path) || $MySQL::base_stxtsz + get_real_size($tablestat));
                    $fileinfo[2] &= ~0222;
                } elsif ($path[2] eq 'create') {
                    set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path) || length MySQL::get_create_table($path[1]));
                    $fileinfo[2] &= ~0222;
                } else {
                    return -ENOENT();
                }
            }
        } elsif ($#path == 3) { # dir-indexes, records, fields
            if ($path[2] eq 'data') {
                my $record = get_record_by_file_name(\@path, 1);
                return -ENOENT() unless $record;# && %$record;
                set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path) || $MySQL::base_rtxtsz{$path[1]} + get_real_size($record));
            } elsif ($path[2] eq 'struct') {
                my $tableinfo = MySQL::get_table_info($path[1], $path[3]);
                return -ENOENT() unless $tableinfo;
                set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path) || ($MySQL::base_itxtsz{$tableinfo->{'Type'}}||$MySQL::def_base_itxtsz) + get_real_size($tableinfo));
            } elsif ($path[2] eq 'indeces') {
                unless (exists $new_indexes{$path[1]}->{$path[3]}) {
                    my $indexinfo = MySQL::get_index_info($path[1], $path[3]);
                    return -ENOENT() unless $indexinfo;
                    set_dir_info(\@fileinfo, scalar @{ $indexinfo->{'Column_name'} });
                    $fileinfo[2] |= S_ISVTX if $indexinfo->{'Unique'};
                } else {
                    $fileinfo[2] = $new_indexes{$path[1]}->{$path[3]};
                    set_dir_info(\@fileinfo, 0);
                }
            }
        } elsif ($#path == 4 && $path[2] eq 'indeces') { # field info
            my @indexinfo = MySQL::get_index_info($path[1], $path[3]);
            my $i = $#indexinfo; foreach (@indexinfo) { last if $_ eq $path[4]; $i--; }
            return -ENOENT() if $i < 0;
            $fileinfo[2] |= S_IFLNK;
            $fileinfo[7] = 21 + length($path[4]) - index($path[4], $fn_sep);
            $fileinfo[9] += 86400 * $i
        }
    }

    return @fileinfo;
}

sub getdir {
    my $dir = shift;

    my @dir_list;

    if ($dir eq '/') {
        @dir_list = ('.query', '.queries', MySQL::get_table_list());
    } elsif ($dir eq '/queries') {
        @dir_list = keys %queries;
    } else {
        my @path = split /\//, $dir;
        if ($#path == 1) { # spec dirs list
            @dir_list = ('create', 'status', 'struct', 'data', 'indeces');
        } else {
            if ($#path == 2) { # list of indexes/records/fields
                if ($path[2] eq 'data') {
                    @dir_list = MySQL::get_table_data($path[1]);
                } elsif ($path[2] eq 'struct') {
                    @dir_list = MySQL::get_table_info($path[1]);
                } elsif ($path[2] eq 'indeces') {
                    @dir_list = ( MySQL::get_index_info($path[1]), keys %{ $new_indexes{$path[1]} } );
                } else {
                    return -ENOENT();
                }
            } elsif ($#path == 3) { # list of key fields
                return -ENOENT() unless $path[2] eq 'indeces';
                @dir_list = MySQL::get_index_info($path[1], $path[3]);
            }
        }
    }

    return ('.', '..', @dir_list, 0);
}

# directories
sub mkdir {
    my $dir = shift;
    my $mode = shift;
    my @path = split /\//, $dir;
    if ($#path == 1) { # create table
        return -EEXIST() if $path[1] eq '.queries';
        my @tableinfo = MySQL::get_table_info($path[1]);
        return -EEXIST() if @tableinfo;
        MySQL::create_table($path[1], 'id');
    } elsif ($#path == 3 && $path[2] eq 'indeces') { # create index
        return -EEXIST() if exists $new_indexes{$path[1]}->{$path[3]};
        my @indexinfo = MySQL::get_index_info($path[1], $path[3]);
        return -EEXIST() if @indexinfo;
        $new_indexes{$path[1]}->{$path[3]} = $mode;
    } else {
        return -EACCES();
    }
    return 0;
}

sub mknod {
    my ($file, $mode) = @_;
    my @path = split /\//, $file;

    if ($#path == 3) {
        if ($path[2] eq 'struct') {
            my @tableinfo = MySQL::get_table_info($path[1], $path[3]);
            return -EEXIST() if @tableinfo;
            #return -EINVAL() unless defined
            MySQL::create_field($path[1], $path[3]);
        } elsif ($path[2] eq 'data') {
            my @record = get_record_by_file_name(\@path, 0);
            return -EEXIST() if @record;
            MySQL::create_record($path[1], $path[3]);
        }
    } elsif ($#path == 2 && $path[1] eq '.queries') {
        return -EEXIST() if exists $queries{$path[2]};
        #return -EINVAL() unless $mode & S_IFIFO;
        return -EINVAL() unless $path[2] =~ /^(SELECT|SHOW)/i;

        #my $sth = $dbh->prepare($path[2]);
        #return -EINVAL() unless ($sth->execute());

        #my $buffer = $sth->fetchall_arrayref({});
        #if ($buffer && @$buffer) {
            #$queries{$path[2]} = mktime(localtime());
            #my $cachefile = get_cache_file_by_path(\@path);
            #put_cache($cachefile, YAML::Tiny::Dump($buffer));
        #}
        #$sth->finish();
    } else {
        return -EACCES();
    }

    return 0;
}

# file open/read/write
sub open {
    my ($file, $flags) = @_;
    my @path = split /\//, $file;

    return -EACCES() unless
    ($flags & (O_WRONLY|O_RDWR) && $file eq '/.query')
    ||
    ($#path == 3 && ($path[2] eq 'struct' || $path[2] eq 'data'))
    ||
    ($#path == 2 && ($path[1] eq '.queries' || $path[2] eq 'status' || $path[2] eq 'create'));

    return 0;
}

sub read {
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
                my $tableinfo = MySQL::get_table_info($path[1], $path[3]);
                return -ENOENT() unless $tableinfo;
                $buffer = YAML::Tiny::Dump($tableinfo);
            } elsif ($path[2] eq 'data') {
                my $record = get_record_by_file_name(\@path, 1);
                return -ENOENT() unless $record;
                $buffer = YAML::Tiny::Dump($record);
            } elsif ($path[2] eq 'status') {
                my $tablestatus = MySQL::get_table_stat($path[1]);
                return -ENOENT() unless $tablestatus;
                $buffer = YAML::Tiny::Dump($tablestatus);
            } elsif ($path[2] eq 'create') {
                my $createstatement = MySQL::get_create_table($path[1]);
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

sub readlink {
    my $file = shift;
    my @path = split /\//, $file;
    return -ENOENT() unless $#path == 4 && $path[2] eq 'indeces';
    my ($name) = split /$fn_sep/, $path[4], 2;
    return "../../struct/$name";
}

sub release {
    my ($file, $flags) = @_;

    my @path = split /\//, $file;
    my $cachefile = get_cache_file_by_path(\@path);

    if (($flags & (O_WRONLY|O_RDWR)) && -r $cachefile) {
        my $data;
        my $buffer = get_cache($cachefile);

        if ($file eq '/query') {
            #my @statements = map { s/^--.*//gm; s/\/\*.*\*\///gm; s/^\s+//; s/\s+$//gm; s/^;$//gm; } split /;\n/, $buffer;
            execute_queries(\$buffer);
        } else {

# YAML::Tiny produces list, not hashref, Load will panic on wrong data
            $data = YAML::Tiny->read_string($buffer) if length $buffer > 3;

            if ($data) {
                undef $buffer;
                if ($path[2] eq 'struct') {
                    MySQL::modify_field($path[1], $path[3], $data->[0])
                } elsif ($path[2] eq 'data') {
                    MySQL::save_record($path[1], parse_file_name_to_record($path[1], $path[3]), $data->[0]);
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

sub rename {
    my ($file, $nfile) = @_;
    my @path = split /\//, $file;
    my @npath = split /\//, $nfile;

    return -EACCES() unless $#path == $#npath;
    return -EACCES() if $path[1] eq '.queries';

    return 0 if $path[$#path] eq $npath[$#npath];

    if ($#path == 1) { # rename table
        my @tableinfo = MySQL::get_table_info($path[1]);
        return -ENOENT() unless @tableinfo;
        MySQL::rename_table($path[1], $npath[1]);
    } else {
        return -EACCES() unless $path[1] eq $npath[1];
        if ($#path == 3) { # rename field, index or record
            if ($path[2] eq 'struct') {
                my $tableinfo = MySQL::get_table_info($path[1], $path[3]);
                return -ENOENT() unless $tableinfo;
                change_field($path[1], $path[3], $npath[3]);
            } elsif ($path[2] eq 'data') {

                my $record = get_record_by_file_name(\@path, 0);
                return -ENOENT() unless $record;

                my @nvalues = split /$fn_sep/, $npath[3];
                return -EINVAL() unless scalar @nvalues == scalar keys %$record;

                my $i = 0;
                my %nrecord = map { $_ => $nvalues[$i++] } sort keys %$record;
                MySQL::update_record($path[1], $record, \%nrecord);

            } elsif ($path[2] eq 'indeces') {
                my $indexinfo = MySQL::get_index_info($path[1], $path[3]);
                return -ENOENT() unless $indexinfo;
                MySQL::modify_index($path[1], $npath[3], $indexinfo);
            }
        } elsif ($#path == 4 && $path[2] eq 'indeces') { # change field in index
            my $indexinfo = MySQL::get_index_info($path[1], $path[3]);
            return -ENOENT() unless $indexinfo;
            @{ $indexinfo->{'Column_name'} } = map { $_ eq $path[4]? $npath[4]: $_ } @{ $indexinfo->{'Column_name'} };
            MySQL::modify_index($path[1], $path[3], $indexinfo);
        } else {
            return -EACCES();
        }
    }
    return 0;
}

sub rmdir {
    my $dir = shift;
    my @path = split /\//, $dir;
    if ($#path == 1) { # drop table
        return -EACCES() if $path[1] eq '.queries';
        MySQL::drop_table($path[1]);
    } elsif ($#path == 3) { # drop index
        if (exists $new_indexes{$path[1]}->{$path[3]}) {
            delete $new_indexes{$path[1]}->{$path[3]};
        } else {
            MySQL::drop_index($path[1], $path[3]);
        }
    } else {
        return -EACCES();
    }
    return 0;
}

# symbolic links
sub symlink {
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
        $indexinfo = MySQL::get_index_info($lpath[1], $lpath[3]);
        push @{ $indexinfo->{'Column_name'} }, $lpath[4];
        MySQL::drop_index($lpath[1], $lpath[3]);
    }
    MySQL::create_index($lpath[1], $lpath[3], $indexinfo);
    return 0;
}

sub truncate {
    my $file = shift;

    my @path = split /\//, $file;
    return -EACCES() unless
    ($file eq '/queries')
    ||
    ($#path == 3 && ($path[2] eq 'data' || $path[2] eq 'struct'));

    MySQL::flush_table_cache($path[1]);
    #unlink get_cache_file_by_path(\@path);

    return 0;
}

sub unlink {
    my $file = shift;
    return 0 if $file eq '/query';
    my @path = split /\//, $file;
    if ($#path == 4 && $path[2] eq 'indeces') {
        my $indexinfo = MySQL::get_index_info($path[1], $path[3]);
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
        MySQL::modify_index($path[1], $path[3], $indexinfo);
    } elsif ($#path == 3) {
        if ($path[2] eq 'data') {
            my $condition = parse_file_name_to_record($path[1], $path[3]);
            return -EINVAL() unless $condition;
            MySQL::delete_record($path[1], $condition);
        } elsif ($path[2] eq 'struct') {
            MySQL::drop_field($path[1], $path[3]);
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

sub write {
    my ($file, $buffer, $offset) = @_;

    my @path = split /\//, $file;
    return -EACCES() unless ($file eq '/query') || ($#path == 3 && ($path[2] eq 'struct' || $path[2] eq 'data'));
    my $cachefile = get_cache_file_by_path(\@path);

    put_cache($cachefile, $buffer, $offset);

    return length $buffer;
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

sub parse_file_name_to_record {
    my ($table, $filename) = @_;
    my @keys = MySQL::get_primary_key($table);
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

sub put_cache {
    my ($cachefile, $buffer, $offset) = @_;
    local $/;
    open FCACHE, '>>', $cachefile;
    seek FCACHE, $offset, 0 if $offset;
    print FCACHE $buffer;
    close FCACHE;
}

sub create_record {
    my ($table, $name) = @_;
    my $tableinfo = MySQL::get_table_info($table);
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

    return MySQL::insert_record($table, \%record);

}

sub get_real_size {
    my $obj = shift;
    my @values = ref $obj eq 'ARRAY'? @$obj: values %$obj;
    my $size = 0;
    map { $size += ref $_? get_real_size($_): defined $_ ? (length($_) + 2): 1 } @values;
    return $size;
}

sub get_record_by_file_name {
    my ($path, $full) = @_;
    my $condition = parse_file_name_to_record($path->[1], $path->[3]);
    return undef unless $condition;
    return wantarray? (MySQL::get_record($path->[1], $condition, $full)): MySQL::get_record($path->[1], $condition, $full);
}

sub set_file_info {
    my ($fileinfo, $size) = @_;
    $fileinfo->[2] |= S_IFREG;
    $fileinfo->[7] = 0 + $size;
    #$fileinfo->[12] = $size / $fileinfo->[11] if $fileinfo->[11];
}

##
# Read data from cache file
# @param string cachefile
# @param integer size
# @param integer offset
# @return string
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

1;
