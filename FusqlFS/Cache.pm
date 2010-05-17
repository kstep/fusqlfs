use strict;
use v5.10.0;

package FusqlFS::Cache;

sub new
{
    my $class = shift;
    my %cache;
    tie %cache, $class, @_ if $class->is_needed(@_);
    return \%cache;
}

sub is_needed
{
    return;
}

1;

package FusqlFS::Cache::File;
use base 'FusqlFS::Cache';

use Carp;

use FusqlFS::Base;

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
    open my $dh, $self->[1] or croak "Unable to open cache dir $self->[1]: $@";
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
    return $self->[0]->{$key};
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
    open my $fh, '<', $$self or croak "Unable to open cache file $$self: $@";

    my $buffer;
    read $fh, $buffer, -s $fh or croak "Unable to read cache file $$self: $@";
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

package FusqlFS::Cache::Limited;
use base 'FusqlFS::Cache';

sub is_needed
{
    return $_[1] > 0;
}

sub TIEHASH
{
    my $class = shift;
    my $threshold = shift;

    # real hash, hits count, total count, threshold, cleanups count
    my $self = [ {}, {}, 0, 0+$threshold, 0 ];

    bless $self, $class;
}

sub FETCH
{
    my ($self, $key) = @_;
    if (exists $self->[0]->{$key})
    {
        $self->[1]->{$key}++;
        return $self->[0]->{$key};
    }
}

sub STORE
{
    my ($self, $key, $value) = @_;
    $self->cleanup() if $self->[2] > $self->[3];
    $self->[2]++ unless exists $self->[0]->{$key};
    $self->[0]->{$key} = $value;
    $self->[1]->{$key} = 1;
}

sub DELETE
{
    my ($self, $key) = @_;
    $self->[2]-- if exists $self->[0]->{$key};
    delete $self->[0]->{$key};
    delete $self->[1]->{$key};
}

sub CLEAR
{
    my ($self)= @_;
    $self->[0] = {};
    $self->[1] = {};
    $self->[2] = 0;
}

sub EXISTS
{
    my ($self, $key) = @_;
    return exists $self->[0]->{$key};
}

sub FIRSTKEY
{
    my ($self) = @_;
    my $_ = keys %{$self->[0]};
    each %{$self->[0]};
}

sub NEXTKEY
{
    my ($self, $lastkey) = @_;
    return each %{$self->[0]};
}

sub SCALAR
{
    my ($self) = @_;
    return scalar(%{$self->[0]});
}

sub cleanup
{
    my ($self) = @_;
    my $del_num = $self->[2] - $self->[3] + ($self->[2] >> 3);
    #carp "cleanup: remove $del_num out from $self->[2], threshold is $self->[3]";
    #$self->[5]++;
    my @keys = sort { $self->[1]->{$a} <=> $self->[1]->{$b} } keys %{$self->[1]};
 
    #carp "top 10 least used keys: ", join(", ", @keys[0..9]);
    #carp "top 10 least counts: ", join(", ", @{$self->[1]}{@keys[0..9]});
    #carp "top 10 most used keys: ", join(", ", @keys[-10..-1]);
    #carp "top 10 most counts: ", join(", ", @{$self->[1]}{@keys[-10..-1]});

    $self->[2] -= $del_num;
    #carp "calculated total: $self->[2]";
    #carp "before cleanup: ", scalar(keys(%{$self->[0]}));

    @keys = @keys[0..$del_num-1];
    delete @{$self->[0]}{@keys};
    delete @{$self->[1]}{@keys};

    #carp "after cleanup: ", scalar(keys(%{$self->[0]}));
}

1;
