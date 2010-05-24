use strict;
use v5.10.0;

package FusqlFS::Cache::Limited;
use parent 'FusqlFS::Cache::Base';

=begin testing

#!noinst

ok FusqlFS::Cache::Limited->is_needed(10), 'Limited cache is needed';
ok !FusqlFS::Cache::Limited->is_needed(0), 'Limited cache isn\'t needed';

=end testing
=cut
sub is_needed
{
    return $_[1] > 0;
}

=begin testing

my %cache;
isa_ok tie(%cache, 'FusqlFS::Cache::Limited', 10), 'FusqlFS::Cache::Limited', 'Limited cache tied';

ok !scalar(%cache), 'Cache is empty';

foreach my $n (1..10)
{
    $cache{'test'.$n} = 'value'.$n;
}

ok scalar(%cache), 'Cache is not empty';

foreach my $n (1..10)
{
    ok exists($cache{'test'.$n}), 'Entry '.$n.' exists';
    is $cache{'test'.$n}, 'value'.$n, 'Entry '.$n.' is intact';
}

ok exists($cache{'test10'}), 'Element exists before deletion';
delete $cache{'test10'};
ok !exists($cache{'test10'}), 'Deleted element doesn\'t exist';
is $cache{'test10'}, undef, 'Deleted element is undefined';

%cache = ();

ok !scalar(%cache), 'Cache is empty after cleanup';

foreach my $n (1..1000)
{
    $cache{'test'.$n} = 'value'.$n;
    foreach my $m (1..1000-$n)
    {
        my $x = $cache{'test'.$n};
    }
}

ok exists($cache{'test1'}), 'Most used element exists';
is $cache{'test1'}, 'value1', 'Most used element is intact';
ok !exists($cache{'test999'}), 'Least used element doesn\'t exist';
is $cache{'test999'}, undef, 'Least used element undefined';
cmp_ok length(%cache), '<=', 10, 'Number of items in cache doesn\'t exceed given threshold';

while (my ($key, $val) = each %cache)
{
    like $key, qr/^test[0-9]+$/, 'Iterate: key is '.$key.' intact';
    like $val, qr/^value[0-9]+$/, 'Iterate: value is '.$val.' intact';
    is substr($key, 4), substr($val, 5), 'Iterate: key matches value';
}

=end testing
=cut
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
    return unless exists $self->[0]->{$key};
    $self->[1]->{$key}++;
    return $self->[0]->{$key};
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

