use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Functions;
use parent 'FusqlFS::Artifact';

sub new
{
    my $class = shift;
    my $self = {};

    $self->{list_expr} = $class->expr('SELECT p.proname FROM pg_catalog.pg_proc AS p
                LEFT JOIN pg_catalog.pg_namespace AS ns ON ns.oid = p.pronamespace
            WHERE ns.nspname = \'public\'');

    $self->{get_expr} = $class->expr('SELECT pg_catalog.pg_get_function_result(p.oid) AS result,
                pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
                CASE WHEN p.proisagg THEN NULL ELSE pg_catalog.pg_get_functiondef(p.oid) END AS struct
            FROM pg_catalog.pg_proc AS p WHERE p.proname = ?');

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($name) = @_;
    return $self->one_row($self->{get_expr}, $name);
}

sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr});
}

1;

