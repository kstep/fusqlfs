use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL;
use parent 'FusqlFS::Backend::Base';

use FusqlFS::Backend::PgSQL::Tables;
use FusqlFS::Backend::PgSQL::Views;
use FusqlFS::Backend::PgSQL::Sequences;
use FusqlFS::Backend::PgSQL::Roles;
use FusqlFS::Backend::PgSQL::Queries;

sub init
{
    $_[0]->{subpackages} = {
        tables    => new FusqlFS::Backend::PgSQL::Tables(),
        views     => new FusqlFS::Backend::PgSQL::Views(),
        sequences => new FusqlFS::Backend::PgSQL::Sequences(),
        roles     => new FusqlFS::Backend::PgSQL::Roles(),
        queries   => new FusqlFS::Backend::PgSQL::Queries(),
    };
}

sub dsn
{
    my $self = shift;
    return 'Pg:'.$self->SUPER::dsn(@_);
}

1;

