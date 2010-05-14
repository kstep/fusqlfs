use strict;
use v5.10.0;
use FusqlFS::Base;
use FusqlFS::PgSQL::Tables;
use FusqlFS::PgSQL::Views;
use FusqlFS::PgSQL::Sequences;

package FusqlFS::PgSQL;
use base 'FusqlFS::Base';

sub init
{
    $_[0]->{subpackages} = {
        tables    => new FusqlFS::PgSQL::Tables(),
        views     => new FusqlFS::PgSQL::Views(),
        sequences => new FusqlFS::PgSQL::Sequences(),
    };
}

sub dsn
{
    my $self = shift;
    return 'Pg:'.$self->SUPER::dsn(@_);
}

1;

