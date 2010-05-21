use strict;
use v5.10.0;

use FusqlFS::Backend::Base;

package MySQL::Backend::Base;
use base 'FusqlFS::Backend::Base';

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

