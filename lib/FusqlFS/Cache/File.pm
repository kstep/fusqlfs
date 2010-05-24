use strict;
use v5.10.0;

package FusqlFS::Cache::File;
use parent 'FusqlFS::Cache::Base';
use Carp;

sub is_needed
{
    return $_[1] > 0;
}

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

