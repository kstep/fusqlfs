use strict;
use v5.10.0;

package FusqlFS::Cache::File;
use parent 'FusqlFS::Cache::Base';
use Carp;

=begin testing

#!noinst

ok FusqlFS::Cache::File->is_needed(10), 'File cache is needed';
ok !FusqlFS::Cache::File->is_needed(0), 'File cache isn\'t needed';

=end testing
=cut

sub is_needed
{
    return $_[1] > 0;
}

=begin testing

# Tie tests
my %cache;
isa_ok tie(%cache, 'FusqlFS::Cache::File', 10), 'FusqlFS::Cache::File', 'File cache tied';

ok !scalar(%cache), 'Cache is empty';

# Store & fetch tests
$cache{'shorttest'} = [ 'pkg', 'names', 'entry' ];
$cache{'longtest'}  = [ 'pkg', 'names', 'long entry' ];

# Exists tests
is_deeply $cache{'shorttest'}, [ 'pkg', 'names', 'entry' ], 'Fetch short entry';
is_deeply $cache{'longtest'} , [ 'pkg', 'names', 'long entry' ], 'Fetch long entry';
ok !defined($cache{'unknown'}), 'Unknown entry is undef';

ok scalar(%cache), 'Cache is not empty';

# Rewrite store tests
$cache{'shorttest'} = [ 'pkg', 'names', 'entri' ];
$cache{'longtest'}  = [ 'pkg', 'names', 'very long entry' ];
is_deeply $cache{'shorttest'}, [ 'pkg', 'names', 'entri' ], 'Fetch short entry after rewrite';
is_deeply $cache{'longtest'} , [ 'pkg', 'names', 'very long entry' ], 'Fetch long entry after rewrite';

# Iterate tests
while (my ($key, $val) = each %cache)
{
    if ($key eq 'shorttest')
    {
        is_deeply $val, [ 'pkg', 'names', 'entri' ], 'Fetch short entry (iterating)';
    }
    elsif ($key eq 'longtest')
    {
        is_deeply $val, [ 'pkg', 'names', 'very long entry' ], 'Fetch long entry (iterating)';
    }
    else
    {
        fail "Key-value pair not stored before: $key => $val";
    }
}

# Delete & clear tests
delete $cache{'shorttest'};
ok !exists($cache{'shorttest'}), 'Short entry deleted';
ok !defined($cache{'shorttest'}), 'Short entry undefined';

delete $cache{'longtest'};
ok !exists($cache{'longtest'}), 'Long entry deleted';
ok !defined($cache{'longtest'}), 'Long entry undefined';

ok !scalar(%cache), 'Cache is empty after delete';

$cache{'othertest'} = [ 'pkg', 'names', '' ];
%cache = ();
ok !scalar(%cache), 'Cache is empty after cleanup';

=end testing
=cut

sub TIEHASH
{
    my $class = shift;
    my $threshold = shift;
    # real storage, cache dir, size threshold

    my $cachedir = "/tmp/fusqlfs-$$.cache";
    mkdir $cachedir or croak "Unable to create cache dir $cachedir: $@";

    my $self = [ {}, $cachedir, 0+$threshold ];
    bless $self, $class;
}

sub FETCH
{
    my ($self, $key) = @_;
    return $self->[0]->{$key}||undef;
}

sub STORE
{
    my ($self, $key, $value) = @_;
    $self->[0]->{$key} = $value;
    return if tied $value->[2];

    if (!ref($value->[2]) && length($value->[2]) > $self->[2])
    {
        tie $value->[2], 'FusqlFS::Cache::File::Record', $self->cachefile($key), $value->[2];
    }
}

sub CLEAR
{
    my ($self) = @_;
    $self->[0] = {};
    opendir my $dh, $self->[1] or croak "Unable to open cache dir $self->[1]: $@";
    while (my $file = readdir($dh))
    {
        my $key = "$self->[1]/$file";
        next unless -f $key;
        unlink $key or carp "Unable to remove cache file $key: $@";
    }
    closedir $dh;
}

sub DELETE
{
    my ($self, $key) = @_;
    return unless exists $self->[0]->{$key};
    delete $self->[0]->{$key};
}

sub EXISTS
{
    my ($self, $key) = @_;
    return exists $self->[0]->{$key};
}

sub FIRSTKEY
{
    my ($self) = @_;
    my @keys = keys %{$self->[0]};
    each %{$self->[0]};
}

sub NEXTKEY
{
    my ($self, $lastkey) = @_;
    each %{$self->[0]};
}

sub SCALAR
{
    my ($self) = @_;
    return scalar(%{$self->[0]});
}

sub UNTIE
{
    my ($self) = @_;
    $self->CLEAR();
    rmdir $self->[1] or carp "Unable to remove cache dir $self->[1]: $@";
}

sub DESTROY
{
    $_[0]->UNTIE();
}

sub cachefile
{
    my ($self, $key) = @_;
    $key =~ s/([^a-zA-Z0-9])/sprintf('_%02x'.ord($1))/ge;
    return "$self->[1]/$key";
}

1;

package FusqlFS::Cache::File::Record;
use Carp;

=begin testing

#!req FusqlFS::Cache::File
#!noinst

my $string = '';

use File::Temp qw(:mktemp);
my $tempfile = mktemp('fusqlfs_test_XXXXXXX');

isa_ok tie($string, 'FusqlFS::Cache::File::Record', $tempfile, 'stored value'), 'FusqlFS::Cache::File::Record', 'File cache record tied';
is $string, 'stored value', 'File cache record is sane';
$string = 'new value';
is $string, 'new value', 'File cache record is sane after rewrite';

=end testing
=cut

sub TIESCALAR
{
    my $value = $_[1];
    my $self = bless \$value, $_[0];
    $self->STORE($_[2]) if defined $_[2];
    return $self;
}

sub FETCH
{
    my $self = shift;
    my $size = -s $$self;
    return '' if !$size || $size == 0;

    open my $fh, '<', $$self or croak "Unable to open cache file $$self: $@";

    my $buffer;
    read $fh, $buffer, $size or croak "Unable to read cache file $$self: $@";
    close $fh;

    return $buffer;
}

sub STORE
{
    my $self = shift;

    open my $fh, '>', $$self or croak "Unable to open cache file $$self: $@";

    print $fh $_[0] or croak "Unable to write cache file $$self: $@";
    close $fh;
}

sub UNTIE
{
    unlink ${$_[0]} or carp "Unable to remove cache file ${$_[0]}: $@";
}

sub DESTROY
{
    $_[0]->UNTIE();
}

1;

