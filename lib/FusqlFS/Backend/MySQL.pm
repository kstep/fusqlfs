use strict;
use 5.010;

package FusqlFS::Backend::MySQL;
our $VERSION = "0.005";
use parent 'FusqlFS::Backend::Base';

sub dsn
{
    my $self = shift;
    return 'mysql:'.$self->SUPER::dsn(@_);
}

sub init
{
    my $self = shift;
    $self->{subpackages} = {
    };
}

1;

