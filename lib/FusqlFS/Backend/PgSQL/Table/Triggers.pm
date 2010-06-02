use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Table::Triggers;
use parent 'FusqlFS::Artifact';

sub new
{
    my $class = shift;
    my $self = {};

    # tgtype:
    # 0  1 = for each row
    # 1  2 = before
    # 2  4 = insert
    # 3  8 = delete
    # 4 16 = update
    # after idu stmt = 00011100
    # after iu row   = 00010101
    # before i stmt  = 00000110
    $self->{get_expr} = $class->expr('SELECT pg_catalog.pg_get_triggerdef(t.oid) AS "create.sql",
            p.proname||\'(\'||pg_catalog.pg_get_function_arguments(p.oid)||\')\' AS handler,
            CASE WHEN (t.tgtype & 1) != 0 THEN \'row\' ELSE \'statement\' END AS for_each,
            CASE WHEN (t.tgtype & 2) != 0 THEN \'before\' ELSE \'after\' END AS when,
            ARRAY[
                CASE WHEN (t.tgtype &  4) != 0 THEN \'insert\' END,
                CASE WHEN (t.tgtype &  8) != 0 THEN \'delete\' END,
                CASE WHEN (t.tgtype & 16) != 0 THEN \'update\' END
                ] AS events
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

    my $result = {};

    $data->{events} = [ grep $_, @{$data->{events}} ];

    $result->{handler} = \"../../../../functions/$data->{handler}";
    delete $data->{handler};

    $result->{'create.sql'} = delete($data->{'create.sql'});
    $result->{struct} = $self->dump($data);

    return $result;
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    return $self->all_col($self->{list_expr}, $table);
}

1;
