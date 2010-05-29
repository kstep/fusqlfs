use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Table::Data;
use parent 'FusqlFS::Artifact';

use FusqlFS::Backend::PgSQL::Table::Struct;

sub new
{
    my $class = shift;
    my $self = {};

    $self->{get_primary_expr} = $class->expr("SELECT indkey FROM pg_catalog.pg_index
            WHERE indisprimary AND indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')");

    $self->{query_cache} = {};

    bless $self, $class;
}

=begin testing list

list_ok $_tobj->list('fusqlfs_table'), [];

=end testing
=cut
sub list
{
    my $self = shift;
    my ($table) = @_;
    my @primary_key = $self->get_primary_key($table);
    return undef unless @primary_key;

    my $sth = $self->cexpr('SELECT %s FROM "%s" %s', $self->concat(@primary_key), $table, $self->limit);
    return $self->all_col($sth)||[];
}

sub where_clause
{
    my $self = shift;
    my ($table, $name) = @_;
    my @binds = $self->asplit($name);
    my @primary_key = $self->get_primary_key($table);
    return unless @primary_key && $#primary_key == $#binds;

    return join(' AND ', map { "\"$_\" = ?" } @primary_key), @binds;
}

=begin testing get

is $_tobj->get('fusqlfs_table', '1'), undef;

=end testing
=cut
sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my ($where_clause, @binds) = $self->where_clause($table, $name);

    $self->{query_cache}->{$table} ||= {};
    $self->{query_cache}->{$table}->{$where_clause} ||= $where_clause?
                $self->expr('SELECT * FROM "%s" WHERE %s LIMIT 1', $table, $where_clause):
                $self->expr('SELECT * FROM "%s" %s', $table, $self->limit);

    my $sth = $self->{query_cache}->{$table}->{$where_clause};
    my $result = $self->all_row($sth, @binds);
    return unless @$result;

    $result = $result->[0] if scalar(@$result) == 1;
    return $self->dump($result);
}

=begin testing drop after rename

isnt $_tobj->drop('fusqlfs_table', '2'), undef;
is $_tobj->get('fusqlfs_table', '2'), undef;
is_deeply $_tobj->list('fusqlfs_table'), [];

=end testing
=cut
sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    my ($where_clause, @binds) = $self->where_clause($table, $name);
    return unless $where_clause;

    $self->cdo('DELETE FROM "%s" WHERE %s', [$table, $where_clause], @binds);
}

sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;
    my ($where_clause, @binds) = $self->where_clause($table, $name);
    return unless $where_clause;

    $data = $self->load($data);
    my $template = join ', ', map { "\"$_\" = ?" } keys %$data;
    $self->cdo('UPDATE "%s" SET %s WHERE %s', [$table, $template, $where_clause], values %$data, @binds);
}

=begin testing create after get list

ok $_tobj->create('fusqlfs_table', '1');
is $_tobj->get('fusqlfs_table', '1'), q{---
id: 1
};
is_deeply $_tobj->list('fusqlfs_table'), [ 1 ];

=end testing
=cut
sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    my @primary_key = $self->get_primary_key($table);
    return unless @primary_key;

    my $pholders = '?,' x scalar(@primary_key);
    chop $pholders;
    $self->cdo('INSERT INTO "%s" (%s) VALUES (%s)', [$table, join(', ', @primary_key), $pholders], $self->asplit($name));
}

=begin testing rename after create

isnt $_tobj->rename('fusqlfs_table', '1', '2'), undef;
is $_tobj->get('fusqlfs_table', '1'), undef;
is $_tobj->get('fusqlfs_table', '2'), q{---
id: 2
};
is_deeply $_tobj->list('fusqlfs_table'), [ 2 ];

=end testing
=cut
sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    my @primary_key = $self->get_primary_key($table);
    return unless @primary_key;

    my %data = map { shift(@primary_key) => $_ } $self->asplit($newname);
    $self->store($table, $name, \%data);
}

sub get_primary_key
{
    my $self = shift;
    my ($table) = @_;
    my @result = ();
    my $data = $self->all_col($self->{get_primary_expr}, $table);
    if ($data)
    {
        my $fields = FusqlFS::Backend::PgSQL::Table::Struct->new()->list($table);
        @result = map { $fields->[$_-1] } split / /, $data->[0];
    }
    return @result;
}

1;

__END__

=begin testing SETUP

#!class FusqlFS::Backend::PgSQL::Table::Test

=end testing
