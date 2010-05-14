use strict;
use v5.10.0;
use FusqlFS::Base;

package FusqlFS::PgSQL::Role::Permissions;
use base 'FusqlFS::Base::Interface';

sub get
{
    my $self = shift;
    my ($name) = @_;
    return {
        tables    => {},
        views     => {},
        functions => {},
    };
}

sub list
{
    return [ qw(tables views functions) ];
}

1;

package FusqlFS::PgSQL::Role::Owned;
use base 'FusqlFS::Base::Interface';

1;

package FusqlFS::PgSQL::Roles;
use base 'FusqlFS::Base::Interface';

sub get
{
    my $self = shift;
    my ($name) = @_;
    return {
        owner => \"../../owner",
        owned => new FusqlFS::PgSQL::Role::Owned($self->{dbh}),
        permissions => new FusqlFS::PgSQL::Role::Permissions($self->{dbh}),
        password => sub() {},
        'create.sql' => '',
    };
}

1;

