use strict;
use v5.10.0;

package FusqlFS;

=head1 NAME

FusqlFS - FUSE filesystem to work with database via DBI interface

=head1 SYNOPSIS

    use FusqlFS;

    FusqlFS::init(
        engine   => 'PgSQL',
        host     => 'localhost',
        database => 'postgres',
        user     => 'postgres',
        password => 'pas$w0rd',
        limit    => 100,
        debug    => 2,
    );

    FusqlFS::mount(
        '/path/to/mount/point',
        mountopts => 'allow_other',
        debug     => 2,
        threaded  => 0,
    );

=head1 DESCRIPTION

This package provides basic interface to FusqlFS to initialize L<DBI> backend
and mount it with L<Fuse>.

=head1 SUBROUTINES

=over

=cut

use POSIX qw(:fcntl_h :errno_h mktime);
use Fcntl qw(:mode);
use Carp;
use Fuse;

use FusqlFS::Cache;
use FusqlFS::Backend;

our $fusqlh;
our $def_time;
our $debug;
our $threaded;
our %cache;
our %inbuffer;

our $VERSION = '0.003';

=item init

Initializes L<FusqlFS::Backend> and L<FusqlFS::Cache> subsystems, preparing
all resources to be actually used by L<Fuse> interface.

Input: %options.
Output: $fusqlfs_instance.

=cut
sub init
{
    my $class = shift;
    my %options = @_;

    $threaded = $options{threaded}||0;
    $debug    = $options{debug}||0;
    $def_time = mktime(localtime());
    $fusqlh   = FusqlFS::Backend->new(@_);
    croak "Unable to initialize database backend" unless defined $fusqlh;

    if ($threaded)
    {
        use threads;
        use threads::shared;
    }

    FusqlFS::Cache->init(\%cache, @options{qw(cache_strategy cache_threshold)});
    $SIG{USR1} = sub () { %cache = (); };

    return $class;
}

=item mount

Runs fuse main loop, configured with all required hooks and options to
interface with L<FusqlFS::Backend>.

Input: $mountpoint, $mountopts.

=cut
sub mount
{
    my $class = shift;
    my $mountpoint = shift;
    my $mountopts = shift||'';

    Fuse::main(
        mountpoint => $mountpoint,
        mountopts  => $mountopts,
        threaded   => $threaded,
        debug      => $debug > 2,

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

=item Fuse hooks

L<Fuse/getdir>, L<Fuse/getattr>, L<Fuse/readlink>, L<Fuse/read>, L<Fuse/write>,
L<Fuse/flush>, L<Fuse/open>, L<Fuse/truncate>, L<Fuse/symlink>, L<Fuse/unlink>,
L<Fuse/mkdir>, L<Fuse/rmdir>, L<Fuse/mknod>, L<Fuse/rename>, L<Fuse/fsync>,
L<Fuse/utime>.

See L<Fuse> for details.

=cut
sub getdir
{
    
    my ($path) = @_;
    my $entry = by_path($path);
    return -ENOENT() unless $entry;
    return -ENOTDIR() unless $entry->isdir();
    return ('.', '..', @{$entry->list()}, 0);
}

sub getattr
{
    my ($path) = @_;
    my $entry = by_path($path);
    return -ENOENT() unless $entry;
    return file_struct($entry);
}

sub readlink
{
    my ($path) = @_;
    my $entry = by_path($path);
    return -ENOENT() unless $entry;
    return -EINVAL() unless $entry->islink();
    return ${$entry->get()};
}

sub read
{
    my ($path, $size, $offset) = @_;
    my $entry = by_path($path);
    return -ENOENT() unless $entry;
    return -EINVAL() unless $entry->isfile();

    return $entry->read($offset, $size);
}

sub write
{
    my ($path, $buffer, $offset) = @_;
    my $entry = by_path($path);
    return -ENOENT() unless $entry;
    return -EISDIR() if $entry->isdir();
    return -EINVAL() unless $entry->isfile();
    return -EACCES() unless $entry->writable();

    $inbuffer{$path} ||= $entry->get();
    substr($inbuffer{$path}, $offset, length($buffer)) = $buffer;
    return length($buffer);
}

sub flush
{
    my ($path) = @_;
    my $entry = by_path($path);
    return -ENOENT() unless $entry;

    flush_inbuffer($path, $entry);
    clear_cache($path) unless $entry->ispipe();
    return 0;
}

sub open
{
    my ($path, $mode) = @_;
    my $entry = by_path($path);
    return -ENOENT() unless $entry;
    return -EISDIR() if $entry->isdir();
    return 0;
}

sub truncate
{
    my ($path, $offset) = @_;
    my $entry = by_path($path);
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
    my $origin = by_path($path);
    return -ENOENT() unless $origin;

    my ($tail) = ($path =~ m{/([^/]+)$});
    my $entry = $fusqlh->by_path($symlink, \$tail);
    return -EEXIST() unless $entry->get() == \$tail;

    $entry->store();
    clear_cache($symlink, $entry->depth());
    return 0;
}

sub unlink
{
    my ($path) = @_;
    my $entry = by_path($path);
    return -ENOENT() unless $entry;
    return -EACCES() unless $entry->writable();
    return -EISDIR() if $entry->isdir();

    $entry->drop();
    clear_cache($path, $entry->depth());
    return 0;
}

sub mkdir
{
    my ($path, $mode) = @_;
    my $newdir = {};
    my $entry = $fusqlh->by_path($path, $newdir);
    return -ENOENT() unless $entry;
    return -EEXIST() unless $entry->get() == $newdir;

    $entry->create();
    clear_cache($path, $entry->depth());
    return 0;
}

sub rmdir
{
    my ($path) = @_;
    my $entry = by_path($path);
    return -ENOENT() unless $entry;
    return -EACCES() unless $entry->writable();
    return -ENOTDIR() unless $entry->isdir();

    $entry->drop();
    clear_cache($path, $entry->depth());
    return 0;
}

sub mknod
{
    my ($path, $mode, $dev) = @_;
    my $entry = $fusqlh->by_path($path, '');
    return -ENOENT() unless $entry;
    return -EEXIST() unless $entry->get() eq '';

    $entry->create();
    clear_cache($path, 1+$entry->depth());
    return 0;
}

sub rename
{
    my ($path, $name) = @_;
    my $entry = by_path($path);
    return -ENOENT() unless $entry;
    return -EACCES() unless $entry->writable();

    my $target = $fusqlh->by_path($name, $entry->get());
    return -ENOENT() unless $target;
    return -EEXIST() unless $entry->get() == $target->get();
    return -EACCES() unless $target->writable();
    return -EOPNOTSUPP() unless $entry->pkg()    == $target->pkg()
                             && $entry->depth()  == $target->depth()
                             && $entry->height() == $target->height();

    $entry->move($target);
    clear_cache($path, $entry->depth());
    return 0;
}

sub fsync
{
    my ($path, $flags) = @_;
    my $entry = by_path($path);
    return -ENOENT() unless $entry;

    flush_inbuffer($path, $entry);
    clear_cache($path, $flags? $entry->depth(): undef);
    return 0;
}

sub utime
{
    my ($path, $atime, $mtime) = @_;
    my $entry = by_path($path);
    return -ENOENT() unless $entry;
    return 0;
}

=item fold_path

Folds path by removing "..", "." and other special sequences from it.

Input: $path.
Output $folded_path.

=cut
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

=item file_struct

Creates FUSE-exportable file structure for given entry.

Input: $entry.
Output: @file_struct.

=cut
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
        $fileinfo[3] = 2 + $entry->size();
    }
    elsif ($entry->islink())
    {
        $fileinfo[2] |= S_IFLNK;
        $fileinfo[7] = $entry->size();
    }
    else
    {
        $fileinfo[2] |= S_IFREG;
        $fileinfo[2] |= S_ISVTX if $entry->ispipe();
        $fileinfo[7] = $entry->size();
    }

    unless ($entry->writable())
    {
        $fileinfo[2] &= ~0222;
    }

    return @fileinfo;
}

=item by_path

Gets entry by path, uses cache.

Input: $path.

=cut
sub by_path
{
    my ($path) = @_;
    return $cache{$path} if exists $cache{$path};
    my $entry = $fusqlh->by_path(@_);
    $cache{$path} = $entry if $entry;
    return $entry;
}

=item clear_cache

Invalidates entries cache for given path, optionally recursively invalidating
all paths up to given depth.

Input: $path, $depth=undef.

=cut
sub clear_cache
{
    delete $cache{$_[0]};
    if (defined $_[1])
    {
        my $key = $_[0];
        my $re = "/[^/]+" x $_[1];
        $key =~ s{$re$}{};
        while (my $_ = each %cache)
        {
            next unless /^$key/;
            delete $cache{$_};
        }
    }
}

=item flush_inbuffer

Flushes input buffer for given $entry by given $path.

Input: $path, $entry.

=cut
sub flush_inbuffer
{
    my ($path, $entry) = @_;
    if (exists $inbuffer{$path})
    {
        $entry->write(0, $inbuffer{$path});
        delete $inbuffer{$path};
    }
}

1;

__END__

=back
