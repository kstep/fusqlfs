use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Table::Data;
use parent 'FusqlFS::Interface';

use FusqlFS::Backend::PgSQL::Table::Struct;

=begin testing

require_ok 'FusqlFS::Backend::PgSQL';
my $fusqlh = FusqlFS::Backend::PgSQL->new(host => '', port => '', database => 'fusqlfs_test', user => 'postgres', password => '');
ok $fusqlh, 'Backend initialized';

require_ok 'FusqlFS::Backend::PgSQL::Table::Data';
my $data = FusqlFS::Backend::PgSQL::Table::Data->new();
ok $data, 'Table data module initialized';

require_ok 'FusqlFS::Backend::PgSQL::Tables';
my $tables = FusqlFS::Backend::PgSQL::Tables->new();
ok $tables, 'Tables module initialized';
ok $tables->create('fusqlfs_table'), 'Test table created';
#ok $tables->{subpackages}->{struct}->create('fusqlfs_table', 'testfield');
#ok $tables->{subpackages}->{struct}->store('fusqlfs_table', 'testfield', q{
#---
#});

# List rows
my $rows = $data->list('fusqlfs_table');
ok $rows;
is ref($rows), 'ARRAY';
is scalar(@$rows), 0;

# Add row
ok !defined $data->get('fusqlfs_table', '1');
ok $data->create('fusqlfs_table', '1');
is $data->get('fusqlfs_table', '1'), q{---
id: 1
};
is_deeply $data->list('fusqlfs_table'), [ 1 ];

# Alter row - TODO

# Rename row
ok $data->rename('fusqlfs_table', '1', '2');
ok !defined $data->get('fusqlfs_table', '1');
is $data->get('fusqlfs_table', '2'), q{---
id: 2
};
is_deeply $data->list('fusqlfs_table'), [ 2 ];

# Delete row
ok $data->drop('fusqlfs_table', '2');
ok !defined $data->get('fusqlfs_table', '2');
is scalar(@{$data->list('fusqlfs_table')}), 0;

$tables->drop('fusqlfs_table');

=end testing
=cut

sub new
{
    my $class = shift;
    my $self = {};

    $self->{get_primary_expr} = $class->expr("SELECT indkey FROM pg_catalog.pg_index
            WHERE indisprimary AND indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')");

    $self->{query_cache} = {};

    bless $self, $class;
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    my $primary_key = join " || '.' || ", $self->get_primary_key($table);
    my $sth = $self->cexpr('SELECT %s FROM "%s" %s', $primary_key, $table, $self->limit());
    return $self->all_col($sth)||[];
}

sub where_clause
{
    my $self = shift;
    my ($table, $name) = @_;
    my @binds = split /[.]/, $name;
    my @primary_key = $self->get_primary_key($table);
    return unless $#primary_key == $#binds;
    return join(' AND ', map { "\"$_\" = ?" } @primary_key), @binds;
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my ($where_clause, @binds) = $self->where_clause($table, $name);
    return unless $where_clause;

    $self->{query_cache}->{$table} ||= {};
    $self->{query_cache}->{$table}->{$where_clause} ||= $self->expr('SELECT * FROM "%s" WHERE %s LIMIT 1', $table, $where_clause);

    my $sth = $self->{query_cache}->{$table}->{$where_clause};
    return $self->dump($sth->fetchrow_hashref) if $sth->execute(@binds);
}

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

sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    my @primary_key = $self->get_primary_key($table);
    my $pholders = '?,' x scalar(@primary_key);
    chop $pholders;
    $self->cdo('INSERT INTO "%s" (%s) VALUES (%s)', [$table, join(', ', @primary_key), $pholders], split(/[.]/, $name));
}

sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    my @primary_key = $self->get_primary_key($table);
    my %data = map { shift(@primary_key) => $_ } split /[.]/, $newname;
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

