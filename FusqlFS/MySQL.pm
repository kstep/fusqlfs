use strict;
use v5.10.0;

use FusqlFS::Base;

package MySQL::Base;
use base 'FusqlFS::Base';

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

