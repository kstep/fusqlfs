package FusqlFS;

use strict;

use YAML::Tiny;
use POSIX qw(:fcntl_h :errno_h mktime);
use Fcntl qw(:mode);
use Fuse;

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

our $fusqlh;

our %queries;
our %new_indexes;

our $def_time;

# host port database user password
sub init {
    my %options = @_;

    my $enginename = 'FusqlFS::'.$options{'engine'};
    my $enginefile = 'FusqlFS/'.$options{'engine'}.'.pm';
    require $enginefile;
    $fusqlh = new $enginename (\%options);

    %queries = ();
    %new_indexes = ();

    $def_time = mktime(localtime());

    #$YAML::Syck::Headless = 1;
    #$YAML::Syck::SingleQuote = 1;
    #$YAML::UseHeader = 0;
}

sub mount {
    my $mountpoint = shift;
    my %options = @_;

    Fuse::main(
        'mountpoint' => $mountpoint,
        'mountopts'  => $options{'allow_other'}? 'allow_other': '',
        'debug'      => $options{'debug'} || 0,
        'threaded'   => $options{'threaded'} || 0,

        'getdir'     => \&getdir,
        'getattr'    => \&getattr,
        'mkdir'      => \&mkdir,
        'rmdir'      => \&rmdir,
        'symlink'    => \&symlink,
        'readlink'   => \&readlink,
        'unlink'     => \&unlink,
        'rename'     => \&rename,
        'open'       => \&open,
        'read'       => \&read,
        'mknod'      => \&mknod,
        'chmod'      => \&chmod,
        'truncate'   => \&truncate,
        'write'      => \&write,
        'flush'      => \&flush,
        'fsync'      => \&flush,
        'release'    => \&release,
        'utime'      => \&utime,
    );
}

sub DESTROY {
    unlink $_ foreach glob("/tmp/".lc(__PACKAGE__)."/*.cache");
}

sub chmod {
    my ($file, $mode) = @_;
    my ($_, @path) = split /\//, $file;

    if ($path[0] eq 'tables') {
        return -EACCES() unless $#path == 3 && $path[2] eq 'indeces';
        unless (exists $new_indexes{$path[1]}->{$path[3]}) {
            my $indexinfo = $fusqlh->get_index_info($path[1], $path[3]);
            return -ENOENT() unless $indexinfo;
            my $unique = $mode & S_ISVTX;

            if ($unique != $indexinfo->{'Unique'}) {
                $indexinfo->{'Unique'} = $unique;
                $fusqlh->modify_index($path[1], $path[3], $indexinfo);
            }
        } else {
            $new_indexes{$path[1]}->{$path[3]} = $mode;
        }
    }

    return 0;
}

sub utime {
    my ($file, $atime, $mtime) = @_;
    my ($_, @path) = split /\//, $file;

    if ($path[0] eq 'tables') {
        return -EACCES() unless $#path == 4 && $path[2] eq 'indeces';
        my $indexinfo = $fusqlh->get_index_info($path[1], $path[3]);
        return -ENOENT() unless $indexinfo;
        my $tablestat = $fusqlh->get_table_stat($path[1]);
        my $i = $#{ $indexinfo->{'Column_name'} };
        my %timestamps = map { $_ => $tablestat->{'Update_time'} + 86400 * $i-- } @{ $indexinfo->{'Column_name'} };
        $timestamps{$path[4]} = $mtime;
        $indexinfo->{'Column_name'} = [ sort { $timestamps{$b} <=> $timestamps{$a} } keys %timestamps ];
        $fusqlh->modify_index($path[1], $path[3], $indexinfo);
    }
    return 0;
}

sub flush {
    my $file = shift;
    my ($_, @path) = split /\//, $file;
    if ($#path > 1 && $path[1] eq 'tables') {
        $fusqlh->flush_table_cache($path[2]);
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
    } elsif ($file eq '/tables') {
        set_dir_info(\@fileinfo, scalar $fusqlh->get_table_list());
    } elsif ($file eq '/queries') {
        set_dir_info(\@fileinfo, scalar keys %queries);
    } elsif ($file eq '/query') {
        set_file_info(\@fileinfo, -s get_cache_file_by_path([ '', '/query' ]));
        $fileinfo[2] &= ~0444;
    } else {
        my ($_, @path) = split /\//, $file;

        if ($path[0] eq 'tables') {
            my $tablestat;

            $tablestat = $fusqlh->get_table_stat($path[1]);
            return -ENOENT() unless $tablestat;
            $fileinfo[8] = $tablestat->{'Check_time'} || $def_time;
            $fileinfo[9] = $tablestat->{'Update_time'} || $def_time;
            $fileinfo[10] = $tablestat->{'Create_time'} || $def_time;

            if ($#path == 1) { # tables
                set_dir_info(\@fileinfo, 3);
            } elsif ($#path == 2) { # special dirs
                if ($path[2] eq 'indeces'
                    || $path[2] eq 'struct'
                    || $path[2] eq 'data')
                {
                    set_dir_info(\@fileinfo);
                } elsif ($path[2] eq 'status') {
                    set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path) || $fusqlh->{'base_stxtsz'} + get_real_size($tablestat));
                    $fileinfo[2] &= ~0222;
                } elsif ($path[2] eq 'create') {
                    set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path) || length $fusqlh->get_create_table($path[1]));
                    $fileinfo[2] &= ~0222;
                } else {
                    return -ENOENT();
                }
            } elsif ($#path == 3) { # dir-indexes, records, fields
                if ($path[2] eq 'data') {
                    my $record = get_record_by_file_name(\@path, 1);
                    return -ENOENT() unless $record;# && %$record;
                    set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path) || $fusqlh->{'base_rtxtsz'}->{$path[1]} + get_real_size($record));
                } elsif ($path[2] eq 'struct') {
                    my $tableinfo = $fusqlh->get_table_info($path[1], $path[3]);
                    return -ENOENT() unless $tableinfo;
                    set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path) || ($fusqlh->{'base_itxtsz'}->{$tableinfo->{'Type'}}||$fusqlh->{'def_base_itxtsz'}) + get_real_size($tableinfo));
                } elsif ($path[2] eq 'indeces') {
                    unless (exists $new_indexes{$path[1]}->{$path[3]}) {
                        my $indexinfo = $fusqlh->get_index_info($path[1], $path[3]);
                        return -ENOENT() unless $indexinfo;
                        set_dir_info(\@fileinfo, scalar @{ $indexinfo->{'Column_name'} });
                        $fileinfo[2] |= S_ISVTX if $indexinfo->{'Unique'};
                    } else {
                        $fileinfo[2] = $new_indexes{$path[1]}->{$path[3]};
                        set_dir_info(\@fileinfo, 0);
                    }
                }
            } elsif ($#path == 4 && $path[2] eq 'indeces') { # field info
                my @indexinfo = $fusqlh->get_index_info($path[1], $path[3]);
                my $i = $#indexinfo; foreach (@indexinfo) { last if $_ eq $path[4]; $i--; }
                return -ENOENT() if $i < 0;
                $fileinfo[2] |= S_IFLNK;
                $fileinfo[7] = 21 + length($path[4]) - index($path[4], $fusqlh->{'fn_sep'});
                $fileinfo[9] += 86400 * $i
            }
        } elsif ($path[0] eq 'queries') {
            return -ENOENT() unless exists $queries{$path[1]};
            my $cachefile = get_cache_file_by_path(\@path);
            unless (-e $cachefile) {
                delete $queries{$path[1]};
                return -ENOENT();
            }
            set_file_info(\@fileinfo, -s get_cache_file_by_path(\@path));
            $fileinfo[2] &= ~0222;
        }
    }

    return @fileinfo;
}

sub getdir {
    my $dir = shift;

    my @dir_list;

    if ($dir eq '/') {
        @dir_list = ('query', 'queries', 'tables');
    } elsif ($dir eq '/tables') {
        @dir_list = ($fusqlh->get_table_list());
    } elsif ($dir eq '/queries') {
        @dir_list = keys %queries;
    } else {
        my ($_, @path) = split /\//, $dir;

        if ($path[0] eq 'tables') {
            if ($#path == 1) { # spec dirs list
                @dir_list = ('create', 'status', 'struct', 'data', 'indeces');
            } else {
                if ($#path == 2) { # list of indexes/records/fields
                    if ($path[2] eq 'data') {
                        @dir_list = $fusqlh->get_table_data($path[1]);
                    } elsif ($path[2] eq 'struct') {
                        @dir_list = $fusqlh->get_table_info($path[1]);
                    } elsif ($path[2] eq 'indeces') {
                        @dir_list = ( $fusqlh->get_index_info($path[1]), keys %{ $new_indexes{$path[1]} } );
                    } else {
                        return -ENOENT();
                    }
                } elsif ($#path == 3) { # list of key fields
                    return -ENOENT() unless $path[2] eq 'indeces';
                    @dir_list = $fusqlh->get_index_info($path[1], $path[3]);
                }
            }
        }
    }

    return ('.', '..', @dir_list, 0);
}

# directories
sub mkdir {
    my ($dir, $mode) = @_;
    my ($_, @path) = split /\//, $dir;
    if ($path[0] eq 'tables') {
        if ($#path == 1) { # create table
            my @tableinfo = $fusqlh->get_table_info($path[1]);
            return -EEXIST() if @tableinfo;
            $fusqlh->create_table($path[1], 'id');
        } elsif ($#path == 3 && $path[2] eq 'indeces') { # create index
            return -EEXIST() if exists $new_indexes{$path[1]}->{$path[3]};
            my @indexinfo = $fusqlh->get_index_info($path[1], $path[3]);
            return -EEXIST() if @indexinfo;
            $new_indexes{$path[1]}->{$path[3]} = $mode;
        } else {
            return -EACCES();
        }
    }
    return 0;
}

sub mknod {
    my ($file, $mode) = @_;
    my ($_, @path) = split /\//, $file;

    if ($path[0] eq 'tables') {
        if ($#path == 3) {
            if ($path[2] eq 'struct') {
                my @tableinfo = $fusqlh->get_table_info($path[1], $path[3]);
                return -EEXIST() if @tableinfo;
                #return -EINVAL() unless defined
                $fusqlh->create_field($path[1], $path[3]);
            } elsif ($path[2] eq 'data') {
                my @record = get_record_by_file_name(\@path, 0);
                return -EEXIST() if @record;
                $fusqlh->create_record($path[1], $path[3]);
            }
        } else {
            return -EACCES();
        }
    } elsif ($path[0] eq 'queries') {
        return -EEXIST() if exists $queries{$path[1]};
        #return -EINVAL() unless $mode & S_IFIFO;
        return -EINVAL() unless $path[1] =~ /^(SELECT|EXPLAIN SELECT|SHOW)/i;

        my $buffer = $fusqlh->execute_query($path[1]);
        return -EINVAL() if $buffer < 0;

        if ($buffer && @$buffer) {
            $queries{$path[1]} = mktime(localtime());
            my $cachefile = get_cache_file_by_path(\@path);
            put_cache($cachefile, YAML::Tiny::Dump($buffer));
        }
    }

    return 0;
}

# file open/read/write
sub open {
    my ($file, $flags) = @_;
    my ($_, @path) = split /\//, $file;

    if ($path[0] eq 'tables') {
        return -EACCES() unless
            ($#path == 3 && ($path[2] eq 'struct' || $path[2] eq 'data'))
            || ($#path == 2 && ($path[2] eq 'status' || $path[2] eq 'create'));
    } elsif ($path[0] eq 'query') {
        return -EACCES() unless $flags & (O_WRONLY|O_RDWR);
    }

    return 0;
}

sub read {
    my ($file, $size, $offset) = @_;
    my ($_, @path) = split /\//, $file;
    my $buffer = '';
    my $cachefile = get_cache_file_by_path(\@path);

    if ($path[0] eq 'tables') {

        if (-r $cachefile) {
            $buffer = get_cache($cachefile);
        } else {
            if ($path[2] eq 'struct') {
                my $tableinfo = $fusqlh->get_table_info($path[1], $path[3]);
                return -ENOENT() unless $tableinfo;
                $buffer = YAML::Tiny::Dump($tableinfo);
            } elsif ($path[2] eq 'data') {
                my $record = get_record_by_file_name(\@path, 1);
                return -ENOENT() unless $record;
                $buffer = YAML::Tiny::Dump($record);
            } elsif ($path[2] eq 'status') {
                my $tablestatus = $fusqlh->get_table_stat($path[1]);
                return -ENOENT() unless $tablestatus;
                $buffer = YAML::Tiny::Dump($tablestatus);
            } elsif ($path[2] eq 'create') {
                my $createstatement = $fusqlh->get_create_table($path[1]);
                return -ENOENT() unless $createstatement;
                $buffer = $createstatement;
            } else {
                return -EACCES();
            }
            put_cache($cachefile, $buffer) if $buffer;
        }
        $buffer .= " " x ($size - length $buffer) if $size > length $buffer;
        return substr($buffer, $offset, $size);

    } elsif ($path[0] eq 'queries') {
        return -ENOENT() unless exists $queries{$path[1]};
        $buffer = get_cache($cachefile, $size, $offset) if -s $cachefile;
        return $buffer;
    }
}

sub readlink {
    my $file = shift;
    my ($_, @path) = split /\//, $file;
    if ($path[0] eq 'tables') {
        return -ENOENT() unless $#path == 4 && $path[2] eq 'indeces';
        my ($name) = split /[$fusqlh->{'fn_sep'}]/, $path[4], 2;
        print STDERR "### ",$name,"\n";
        print STDERR "### ",$path[4],"\n";
        return "../../struct/$name";
    }
}

sub release {
    my ($file, $flags) = @_;

    my ($_, @path) = split /\//, $file;

    if ($path[0] eq 'tables') {
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
                        $fusqlh->modify_field($path[1], $path[3], $data->[0])
                    } elsif ($path[2] eq 'data') {
                        $fusqlh->save_record($path[1], parse_file_name_to_record($path[1], $path[3]), $data->[0]);
                    } else {
                        return -EACCES();
                    }
                }# else {
                #	return -EINVAL();
                #}
            }
        }

        unlink $cachefile unless $path[1] eq '.queries';
    }
    return 0;
}

sub rename {
    my ($file, $nfile) = @_;
    my ($_, @path) = split /\//, $file;
    my ($_, @npath) = split /\//, $nfile;
    return -EACCES() unless $path[0] eq @npath[0];

    if ($path[0] eq 'tables') {
        return -EACCES() unless $#path == $#npath;
        return -EACCES() if $path[1] eq '.queries';

        return 0 if $path[$#path] eq $npath[$#npath];

        if ($#path == 1) { # rename table
            my @tableinfo = $fusqlh->get_table_info($path[1]);
            return -ENOENT() unless @tableinfo;
            $fusqlh->rename_table($path[1], $npath[1]);
        } else {
            return -EACCES() unless $path[1] eq $npath[1];
            if ($#path == 3) { # rename field, index or record
                if ($path[2] eq 'struct') {
                    my $tableinfo = $fusqlh->get_table_info($path[1], $path[3]);
                    return -ENOENT() unless $tableinfo;
                    change_field($path[1], $path[3], $npath[3]);
                } elsif ($path[2] eq 'data') {

                    my $record = get_record_by_file_name(\@path, 0);
                    return -ENOENT() unless $record;

                    my @nvalues = split /[$fusqlh->{fn_sep}]/, $npath[3];
                    return -EINVAL() unless scalar @nvalues == scalar keys %$record;

                    my $i = 0;
                    my %nrecord = map { $_ => $nvalues[$i++] } sort keys %$record;
                    $fusqlh->update_record($path[1], $record, \%nrecord);

                } elsif ($path[2] eq 'indeces') {
                    my $indexinfo = $fusqlh->get_index_info($path[1], $path[3]);
                    return -ENOENT() unless $indexinfo;
                    $fusqlh->modify_index($path[1], $npath[3], $indexinfo);
                }
            } elsif ($#path == 4 && $path[2] eq 'indeces') { # change field in index
                my $indexinfo = $fusqlh->get_index_info($path[1], $path[3]);
                return -ENOENT() unless $indexinfo;
                @{ $indexinfo->{'Column_name'} } = map { $_ eq $path[4]? $npath[4]: $_ } @{ $indexinfo->{'Column_name'} };
                $fusqlh->modify_index($path[1], $path[3], $indexinfo);
            } else {
                return -EACCES();
            }
        }
    }

    return 0;
}

sub rmdir {
    my $dir = shift;
    my ($_, @path) = split /\//, $dir;

    if ($path[0] eq 'tables') {
        if ($#path == 1) { # drop table
            return -EACCES() if $path[1] eq '.queries';
            $fusqlh->drop_table($path[1]);
        } elsif ($#path == 3) { # drop index
            if (exists $new_indexes{$path[1]}->{$path[3]}) {
                delete $new_indexes{$path[1]}->{$path[3]};
            } else {
                $fusqlh->drop_index($path[1], $path[3]);
            }
        } else {
            return -EACCES();
        }
    }
    return 0;
}

# symbolic links
sub symlink {
    my ($file, $link) = @_;
    my ($_, @path) = split /\//, $file;
    my ($_, @lpath) = split /\//, $link;
    return -EACCES() unless $path[0] eq $lpath[0];

    if ($path[0] eq 'tables') {
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

        my @name = split /[$fusqlh->{fn_sep}]/, $lpath[4];
        return -EINVAL() unless $#name < 2 && $name[0] eq $path[3];

        my $indexinfo;
        if (exists $new_indexes{$lpath[1]}->{$lpath[3]}) {
            $indexinfo = { 'Unique' => $new_indexes{$lpath[1]}->{$lpath[3]} & S_ISVTX, 'Column_name' => [ $lpath[4] ] };
            delete $new_indexes{$lpath[1]}->{$lpath[3]};
        } else {
            $indexinfo = $fusqlh->get_index_info($lpath[1], $lpath[3]);
            push @{ $indexinfo->{'Column_name'} }, $lpath[4];
            $fusqlh->drop_index($lpath[1], $lpath[3]);
        }
        $fusqlh->create_index($lpath[1], $lpath[3], $indexinfo);
    }

    return 0;
}

sub truncate {
    my $file = shift;

    my ($_, @path) = split /\//, $file;
    return -EACCES() unless $path[0] eq 'tables';
    return -EACCES() unless
        ($file eq '/tables/queries')
        ||
        ($#path == 3 && ($path[2] eq 'data' || $path[2] eq 'struct'));

    $fusqlh->flush_table_cache($path[1]);
    #unlink get_cache_file_by_path(\@path);

    return 0;
}

sub unlink {
    my $file = shift;
    return 0 if $file eq '/query';
    my ($_, @path) = split /\//, $file;
    if ($#path == 4 && $path[2] eq 'indeces') {
        my $indexinfo = $fusqlh->get_index_info($path[1], $path[3]);
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
        $fusqlh->modify_index($path[1], $path[3], $indexinfo);
    } elsif ($#path == 3) {
        if ($path[2] eq 'data') {
            my $condition = parse_file_name_to_record($path[1], $path[3]);
            return -EINVAL() unless $condition;
            $fusqlh->delete_record($path[1], $condition);
        } elsif ($path[2] eq 'struct') {
            $fusqlh->drop_field($path[1], $path[3]);
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
    my ($_, @path) = split /\//, $file;

    if ($path[0] eq 'tables') {
        return -EACCES() unless ($file eq '/query') || ($#path == 3 && ($path[2] eq 'struct' || $path[2] eq 'data'));
        my $cachefile = get_cache_file_by_path(\@path);

        put_cache($cachefile, $buffer, $offset);
    }

    return length $buffer;
}

sub get_cache_file_by_path {
    my $path = shift;
    my $file = "/tmp/".lc(__PACKAGE__);
# e.g. /tmp/mysqlfs.not_auth.id..struct"
    if ($path->[1] eq '/query') {
        return "$file.query.cache";
    } elsif ($path->[0] eq 'queries') {
        return "$file.$queries{$path->[1]}.queries.cache";
    } else {
        return "$file.$path->[1].$path->[3].$path->[2]";
    }
}

sub parse_file_name_to_record {
    my ($table, $filename) = @_;
    my @keys = $fusqlh->get_primary_key($table);
    my @values = split /[$fusqlh->{fn_sep}]/, $filename, scalar @keys;
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
    return wantarray? ($fusqlh->get_record($path->[1], $condition, $full)): $fusqlh->get_record($path->[1], $condition, $full);
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
