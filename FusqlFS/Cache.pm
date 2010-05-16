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
    return unless exists $self->[0]->{$key};

    my $value = $self->[0]->{$key};
    return $value if ref $value || $value ne "\000";

    $key = $self->cachefile($key);
    open my $fh, '<', $key or croak "Unable to open cache file $key: $@";

    my $buffer;
    read $fh, $buffer, -s $fh or croak "Unable to read cache file $key: $@";
    close $fh;

    return $buffer;
}

sub STORE
{
    my ($self, $key, $value) = @_;
    unless (!ref $value && length($value) > $self->[2])
    {
        $self->[0]->{$key} = $value;
    }
    else
    {
        $self->[0]->{$key} = "\000";

        $key = $self->cachefile($key);
        open my $fh, '>', $key or croak "Unable to open cache file $key: $@";

        print $fh $value or croak "Unable to write cache file $key: $@";
        close $fh;
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
    if ($self->[0]->{$key} eq "\000")
    {
        $key = $self->cachefile($key);
        unlink "$self->[1]/$key";
    }
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

sub cachefile
{
    my ($self, $key) = @_;
    $key =~ s/([^a-zA-Z0-9])/sprintf('_%02x'.ord($1))/ge;
    return "$self->[1]/$key";
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
