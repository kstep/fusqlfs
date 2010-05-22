use strict;
use v5.10.0;

package FusqlFS::Cache::Base;

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

