use strict;
use 5.010;

package FusqlFS::Backend::MySQL;
our $VERSION = "0.005";
use parent 'FusqlFS::Backend::Base';

use FusqlFS::Backend::MySQL::Tables;

sub init
{
    my $self = shift;
    $self->{subpackages} = {
        tables => FusqlFS::Backend::MySQL::Tables->new(),
    };
}

sub dsn
{
    my $self = shift;
    return 'mysql:'.$self->SUPER::dsn(@_);
}

1;

