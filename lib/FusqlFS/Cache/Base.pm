use strict;
use v5.10.0;

package FusqlFS::Cache::Base;

=begin testing new

#!noinst

my $test = $_tcls->new();
isa_ok $test, 'HASH';
is tied($test), undef;

=end testing
=cut

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

