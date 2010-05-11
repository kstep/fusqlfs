use v5.10.0;
use strict;

package FusqlFS;
use POSIX qw(:fcntl_h :errno_h mktime);
use Fcntl qw(:mode);
use Data::Dump qw(dump);
use Fuse;

our $fusqlh;
our $def_time;

sub dbg
{
    my $caller = caller();
    local $, = ",";
    print STDERR $caller, @_, "\n";
}

sub init
{
    my %options = @_;

    my $engine = $options{engine};

    my $filename = "FusqlFS/${engine}.pm";
    my $package = "FusqlFS::${engine}";

    require $filename;
    $fusqlh = $package->new(@options{qw(host port database user password)});
    $def_time = mktime(localtime());
}

sub mount
{
    my $mountpoint = shift;
    my %options = @_;

    Fuse::main(
        mountpoint => $mountpoint,
        mountopts  => $options{'allow_other'}? 'allow_other': '',
        debug      => $options{'debug'} || 0,

        getdir     => \&getdir,
        getattr    => \&getattr,
        readlink   => \&readlink,
        read       => \&read,

        write      => \&write,
        truncate   => \&truncate,
        flush      => \&flush,
        symlink    => \&symlink,
    );
}

sub getdir
{
    
    my ($path) = @_;
    my $entry = $fusqlh->by_path($path);
    return ('.', '..', @{$entry->list()}, 0);
}

sub getattr
{
    my ($path) = @_;
    my $entry = $fusqlh->by_path($path);
    return -ENOENT() unless $entry;
    return file_struct($entry);
}

sub readlink
{
    my ($path) = @_;
    my $entry = $fusqlh->by_path($path);
    return ${$entry->get()};
}

sub read
{
    my ($path, $size, $offset) = @_;
    my $entry = $fusqlh->by_path($path);
    return substr($entry->get(), $offset, $size);
}

sub write
{
    my ($path, $buffer, $offset) = @_;
    my $entry = $fusqlh->by_path($path);
    $entry->write($offset, $buffer);
    return length($buffer);
}

sub flush
{
    my ($path) = @_;
    my $entry = $fusqlh->by_path($path);
    if ($entry->isdirty())
    {
        $entry->flush();
        $fusqlh->clear_cache($path);
    }
    return 0;
}

sub open
{
    my ($path, $mode) = @_;
    return 0;
}

sub truncate
{
    my ($path, $offset) = @_;
    my $entry = $fusqlh->by_path($path);
    $entry->write($offset);
    return 0;
}

sub symlink
{
    $, = ",";
    my ($path, $symlink) = @_;
    say STDERR "path", $path, "symlink", $symlink;
    my $entry = $fusqlh->by_path($symlink, \$path);
    say STDERR "symlink", $entry;
    return -EACCES();
}

sub file_struct
{
    my ($entry) = @_;
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
    if ($entry->isdir())
    {
        $fileinfo[2] |= (S_IFDIR|0111);
        $fileinfo[3] = 2;
    }
    elsif ($entry->islink())
    {
        $fileinfo[2] |= S_IFLNK;
        $fileinfo[7] = 0 + length(${$entry->get()});
    }
    else
    {
        $fileinfo[2] |= S_IFREG;
        $fileinfo[7] = 0 + length($entry->get());
    }

    return @fileinfo;
}

1;
