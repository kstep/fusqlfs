use strict;
use v5.10.0;

use FusqlFS::Base;

package FusqlFS::PgSQL::Tables;
use base 'FusqlFS::Base::Interface';

use FusqlFS::PgSQL::Roles;
use FusqlFS::PgSQL::Table::Indices;
use FusqlFS::PgSQL::Table::Struct;
use FusqlFS::PgSQL::Table::Data;
use FusqlFS::PgSQL::Table::Constraints;

sub new
{
    my $class = shift;
    my $self = {};
    $self->{rename_expr} = 'ALTER TABLE "%s" RENAME TO "%s"';
    $self->{drop_expr} = 'DROP TABLE "%s"';
    $self->{create_expr} = 'CREATE TABLE "%s" (id serial, PRIMARY KEY (id))';

    $self->{list_expr} = $class->expr("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'");
    $self->{get_expr} = $class->expr("SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename = ?");

    $self->{subpackages} = {
        indices     => new FusqlFS::PgSQL::Table::Indices(),
        struct      => new FusqlFS::PgSQL::Table::Struct(),
        data        => new FusqlFS::PgSQL::Table::Data(),
        constraints => new FusqlFS::PgSQL::Table::Constraints(),
        owner       => new FusqlFS::PgSQL::Role::Owner('r', 2),
    };

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($name) = @_;
    my $result = $self->all_col($self->{get_expr}, $name);
    return unless @$result;
    return $self->{subpackages};
}

sub drop
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{drop_expr}, [$name]);
}

sub create
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{create_expr}, [$name]);
}

sub rename
{
    my $self = shift;
    my ($name, $newname) = @_;
    $self->do($self->{rename_expr}, [$name, $newname]);
}

sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr}) || [];
}

1;

