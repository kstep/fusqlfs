use strict;
use v5.10.0;
use FusqlFS::Base;
use FusqlFS::PgSQL::Tables;
use FusqlFS::PgSQL::Views;
use FusqlFS::PgSQL::Sequences;
use FusqlFS::PgSQL::Roles;
use FusqlFS::PgSQL::Queries;

package FusqlFS::PgSQL;
use base 'FusqlFS::Base';

sub init
{
    $_[0]->{subpackages} = {
        tables    => new FusqlFS::PgSQL::Tables(),
        views     => new FusqlFS::PgSQL::Views(),
        sequences => new FusqlFS::PgSQL::Sequences(),
        roles     => new FusqlFS::PgSQL::Roles(),
        queries   => new FusqlFS::PgSQL::Queries(),
    };
}

sub dsn
{
    my $self = shift;
    return 'Pg:'.$self->SUPER::dsn(@_);
}

1;

