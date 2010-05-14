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

sub new
{
    my $class = shift;
    my $self = {};

    $self->{'list_expr'} = $class->expr("SELECT rolname FROM pg_catalog.pg_roles");
    $self->{'get_expr'} = $class->expr("SELECT * FROM pg_catalog.pg_roles WHERE rolname = ?");

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($name) = @_;
    return $self->dump($self->one_row($self->{'get_expr'}, $name));
}

sub list
{
    my $self = shift;
    return $self->all_col($self->{'list_expr'})||[];
}

1;

