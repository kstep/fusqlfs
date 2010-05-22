use strict;
use v5.10.0;

use FusqlFS::Interface;

package FusqlFS::Backend::PgSQL::Table::Indices;
use base 'FusqlFS::Interface';

use FusqlFS::Backend::PgSQL::Table::Struct;

sub new
{
    my $class = shift;
    my $self = {};
    $self->{rename_expr} = 'ALTER INDEX "%s" RENAME TO "%s"';
    $self->{drop_expr} = 'DROP INDEX "%s"';
    $self->{create_expr} = 'CREATE %s INDEX "%s" ON "%s" (%s)';

    $self->{list_expr} = $class->expr("SELECT (SELECT c1.relname FROM pg_catalog.pg_class as c1 WHERE c1.oid = indexrelid) as Index_name
        FROM pg_catalog.pg_index
            WHERE indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')");
    $self->{get_expr} = $class->expr("SELECT pg_get_indexdef(indexrelid, 0, true) AS \"create.sql\",
            indisunique as \".unique\", indisprimary as \".primary\", indkey as \".order\"
        FROM pg_catalog.pg_index
            WHERE indexrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'i')");

    $self->{create_cache} = {};

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    return $self->{create_cache}->{$table}->{$name} if exists $self->{create_cache}->{$table}->{$name};

    my $result = $self->one_row($self->{get_expr}, $name);
    return unless $result;
    if ($result->{'.order'})
    {
        my @fields = @{FusqlFS::Backend::PgSQL::Table::Struct->new()->list($table)};
        $result->{'.order'} = [ map { $fields[$_-1] } split / /, $result->{'.order'} ];
        $result->{$_} = \"../../struct/$_" foreach @{$result->{'.order'}};
    }
    delete $result->{'.unique'} unless $result->{'.unique'};
    delete $result->{'.primary'} unless $result->{'.primary'};
    return $result;
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    my @list = keys %{$self->{create_cache}->{$table}||{}};
    return [ (@{$self->all_col($self->{list_expr}, $table)}, @list) ] || \@list;
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->do($self->{drop_expr}, [$name]);
}

sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;
    if (exists $self->{create_cache}->{$table}->{$name})
    {
        delete $self->{create_cache}->{$table}->{$name};
    }
    else
    {
        $self->drop($table, $name);
    }
    my $fields = $self->parse_fields($data);
    my $unique = defined $data->{'.unique'}? 'UNIQUE': '';
    $self->do($self->{create_expr}, [$unique, $name, $table, $fields]);
}

sub parse_fields
{
    my $self = shift;
    my ($data) = @_;
    my @order = grep { exists $data->{$_} } @{$data->{'.order'}};
    my @fields = grep { !/^\./ && $_ ne 'create.sql' } keys %$data;

    my %order = map { $_ => 1 } @order;
    foreach (@fields)
    {
        push @order, $_ unless exists $order{$_};
    }
    my $fields = '"'.join('", "', @order).'"';

    return $fields;
}

sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->{create_cache}->{$table} ||= {};
    $self->{create_cache}->{$table}->{$name} = { '.order' => [] };
}

sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    $self->do($self->{rename_expr}, [$name, $newname]);
}

1;

