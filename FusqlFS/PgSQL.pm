use strict;
use v5.10.0;
use FusqlFS::Base;
use FusqlFS::PgSQL::Tables;
use FusqlFS::PgSQL::Views;

package FusqlFS::PgSQL;
use base 'FusqlFS::Base';

sub init
{
    $_[0]->{subpackages} = {
        tables => new FusqlFS::PgSQL::Tables(),
        views  => new FusqlFS::PgSQL::Views(),
    };
}

sub dsn
{
    my $self = shift;
    return 'Pg:'.$self->SUPER::dsn(@_);
}

1;

