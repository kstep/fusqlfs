package FusqlFS;

use strict;

use YAML::Tiny;
use POSIX qw(:fcntl_h :errno_h mktime);
use Fcntl qw(:mode);
use Fuse;

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
    return 0;
}

sub utime {
    my ($file, $atime, $mtime) = @_;
    return 0;
}

sub flush {
    my $file = shift;
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

    return @fileinfo;
}

sub getdir {
    my $dir = shift;

    my @dir_list;

    return ('.', '..', @dir_list, 0);
}

# directories
sub mkdir {
    my ($dir, $mode) = @_;
    return 0;
}

sub mknod {
    my ($file, $mode) = @_;

    return 0;
}

# file open/read/write
sub open {
    my ($file, $flags) = @_;
    return 0;
}

sub read {
    my ($file, $size, $offset) = @_;
}

sub readlink {
    my $file = shift;
}

sub release {
    my ($file, $flags) = @_;
    return 0;
}

sub rename {
    my ($file, $nfile) = @_;
    return 0;
}

sub rmdir {
    my $dir = shift;
    return 0;
}

# symbolic links
sub symlink {
    my ($file, $link) = @_;
    return 0;
}

sub truncate {
    my $file = shift;
    return 0;
}

sub unlink {
    my $file = shift;
    return 0;
}

sub write {
    my ($file, $buffer, $offset) = @_;
    return length $buffer;
}

# @param string path
# @return hashref object
sub map_path_to_obj {
    my ($_, @path) = split /\//, shift;
    my $entry = $fusqlh;
    my $refmode = 0;
    while (@path) {
        my $p = shift @path;
        my $ref = ref $entry;
        return 0 unless $ref;
        if (exists $entry->{'.mods'}) {
            $entry = $entry->{'.mods'}->{$p};
        } elsif (exists $entry->{'.refs'}) {
            $entry = $entry->{'.refs'}->{$p};
            $refmode = 1;
        } elsif ($refmode) {
            $entry = $entry->{$p};
        } elsif ($entry->can('get')) {
            $entry = $entry->get($p);
        } else {
            return 0;
        }
    }
    return $entry;
}

# @param hashref object
# @return string path
sub map_obj_to_path {
    my $entry = shift;
    my @path = ();
    while ($entry != $fusqlh) {
        my $ref = ref $entry;
        my $name;
        if ($ref =~ /^FusqlFS::/) {
            ($name) = ($ref =~ /([^:]+)$/);
        } elsif ($ref eq 'HASH') {
            $name = $entry->{'name'};
        } else {
            return 0;
        }
        unshift @path, lc $name;
        $entry = $entry->{'parent'};
    }
    return '/'.join('/', $path);
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
