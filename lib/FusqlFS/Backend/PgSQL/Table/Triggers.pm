use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Table::Triggers;
use parent 'FusqlFS::Artifact';

sub new
{
    my $class = shift;
    my $self = {};

    $self->{get_expr} = $class->expr('SELECT pg_catalog.pg_get_triggerdef(t.oid) AS "create.sql",
            p.proname||\'(\'||pg_catalog.pg_get_function_arguments(p.oid)||\')\' AS handler
        FROM pg_catalog.pg_trigger AS t
            LEFT JOIN pg_catalog.pg_class AS r ON (t.tgrelid = r.oid)
            LEFT JOIN pg_catalog.pg_proc AS p ON (t.tgfoid = p.oid)
        WHERE r.relname = ? AND t.tgname = ? AND t.tgconstraint = 0');
    $self->{list_expr} = $class->expr('SELECT t.tgname FROM pg_catalog.pg_trigger AS t
        LEFT JOIN pg_catalog.pg_class AS r ON (t.tgrelid = r.oid) WHERE r.relname = ?');

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $data = $self->one_row($self->{get_expr}, $table, $name);
    return unless $data;

    $data->{handler} = \"../../../../functions/$data->{handler}";
    return $data;
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    return $self->all_col($self->{list_expr}, $table);
}

1;
