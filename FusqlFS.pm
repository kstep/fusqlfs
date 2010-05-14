use v5.10.0;
use strict;

package FusqlFS;
use POSIX qw(:fcntl_h :errno_h mktime);
use Fcntl qw(:mode);
use Data::Dump qw(dump);
use Fuse;

our $fusqlh;
our $def_time;

sub init
{
    my %options = @_;

    my $engine = $options{engine};

    my $filename = "FusqlFS/${engine}.pm";
    my $package = "FusqlFS::${engine}";

    require $filename;
    $fusqlh = $package->new(@_);
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
        open       => \&open,
        read       => \&read,

        write      => \&write,
        truncate   => \&truncate,
        flush      => \&flush,
        rename     => \&rename,

        mkdir      => \&mkdir,
        mknod      => \&mknod,
        symlink    => \&symlink,

        unlink     => \&unlink,
        rmdir      => \&rmdir,

        fsync      => \&fsync,
        utime      => \&utime,
    );
}

sub getdir
{
    
    my ($path) = @_;
    my $entry = $fusqlh->by_path($path);
    return -ENOENT() unless $entry;
    return -ENOTDIR() unless $entry->isdir();
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
    return -ENOENT() unless $entry;
    return -EINVAL() unless $entry->islink();
    return ${$entry->get()};
}

sub read
{
    my ($path, $size, $offset) = @_;
    my $entry = $fusqlh->by_path($path);
    return -ENOENT() unless $entry;
    return -EINVAL() unless $entry->isfile();
    return substr($entry->get(), $offset, $size);
}

sub write
{
    my ($path, $buffer, $offset) = @_;
    my $entry = $fusqlh->by_path($path);
    return -ENOENT() unless $entry;
    return -EINVAL() unless $entry->isfile();
    return -EACCES() unless $entry->writable();

    $entry->write($offset, $buffer);
    return length($buffer);
}

sub flush
{
    my ($path) = @_;
    my $entry = $fusqlh->by_path($path);
    return -ENOENT() unless $entry;

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
    my $entry = $fusqlh->by_path($path);
    return -ENOENT() unless $entry;
    return -EISDIR() if $entry->isdir();
    return 0;
}

sub truncate
{
    my ($path, $offset) = @_;
    my $entry = $fusqlh->by_path($path);
    return -ENOENT() unless $entry;
    return -EINVAL() unless $entry->isfile();
    return -EACCES() unless $entry->writable();

    $entry->write($offset);
    return 0;
}

sub symlink
{
    my ($path, $symlink) = @_;
    return -EOPNOTSUPP() if $path =~ /^\//;

    $path = fold_path($symlink, '..', $path);
    my $origin = $fusqlh->by_path($path);
    return -ENOENT() unless $origin;

    my ($tail) = ($path =~ m{/([^/]+)$});
    my $entry = $fusqlh->by_path_uncached($symlink, \$tail);
    return -EEXIST() unless $entry->get() == \$tail;

    $entry->store();
    $fusqlh->clear_cache($symlink, $entry->depth());
    return 0;
}

sub unlink
{
    my ($path) = @_;
    my $entry = $fusqlh->by_path($path);
    return -ENOENT() unless $entry;
    return -EACCES() unless $entry->writable();

    $entry->drop();
    $fusqlh->clear_cache($path, $entry->depth());
    return 0;
}

sub mkdir
{
    my ($path, $mode) = @_;
    my $newdir = {};
    my $entry = $fusqlh->by_path_uncached($path, $newdir);
    return -ENOENT() unless $entry;
    return -EEXIST() unless $entry->get() == $newdir;

    $entry->create();
    $fusqlh->clear_cache($path, $entry->depth());
    return 0;
}

sub rmdir
{
    my ($path) = @_;
    my $entry = $fusqlh->by_path($path);
    return -ENOENT() unless $entry;
    return -EACCES() unless $entry->writable();

    $entry->drop();
    $fusqlh->clear_cache($path, $entry->depth());
    return 0;
}

sub mknod
{
    my ($path, $mode, $dev) = @_;
    my $entry = $fusqlh->by_path_uncached($path, '');
    return -ENOENT() unless $entry;
    return -EEXIST() unless $entry->get() eq '';

    $entry->create();
    $fusqlh->clear_cache($path, $entry->depth());
    return 0;
}

sub rename
{
    my ($path, $name) = @_;
    my $entry = $fusqlh->by_path($path);
    return -ENOENT() unless $entry;
    return -EACCES() unless $entry->writable();

    my $target = $fusqlh->by_path_uncached($name, $entry->get());
    return -ENOENT() unless $target;
    return -EEXIST() unless $entry->get() == $target->get();
    return -EACCES() unless $target->writable();
    return -EOPNOTSUPP() unless $entry->pkg()    == $target->pkg()
                             && $entry->depth()  == $target->depth()
                             && $entry->height() == $target->height();

    $entry->move($target);
    $fusqlh->clear_cache($path, $entry->depth());
    return 0;
}

sub fsync
{
    my ($path, $flags) = @_;
    my $entry = $fusqlh->by_path($path);
    return -ENOENT() unless $entry;

    $entry->flush();
    $fusqlh->clear_cache($path, $flags? undef: $entry->depth());
    return 0;
}

sub utime
{
    my ($path, $atime, $mtime) = @_;
    my $entry = $fusqlh->by_path($path);
    return -ENOENT() unless $entry;
    return 0;
}

sub fold_path
{
    local $/ = '/';
    my $path = join '/', @_;
    $path =~ s{//+}{/}g;
    while ($path =~ s{/\./}{/}g) {}
    while ($path =~ s{[^/]+/\.\./}{}) {}
    #$path =~ s{^/\.\./}{/};
    chomp $path;
    return $path;
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
        $fileinfo[3] = 2 + scalar(@{$entry->list()});
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

    unless ($entry->writable())
    {
        $fileinfo[2] &= ~0222;
    }

    return @fileinfo;
}

1;
