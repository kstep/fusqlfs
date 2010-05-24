use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Tables;
use parent 'FusqlFS::Interface';

=begin testing

require_ok 'FusqlFS::Backend::PgSQL';
my $fusqlh = FusqlFS::Backend::PgSQL->new(host => '', port => '', database => 'fusqlfs_test', user => 'postgres', password => '');
ok $fusqlh, 'Backend initialized';

require_ok 'FusqlFS::Backend::PgSQL::Tables';
my $tables = FusqlFS::Backend::PgSQL::Tables->new();

# List tables
my $list = $tables->list();
ok $list, 'Tables list is sane';
is ref($list), 'ARRAY', 'Tables list is an array';
is scalar(@$list), 0, 'Tables list is empty';

# Get & create table
ok !defined($tables->get('fusqlfs_table')), 'Test table doesn\'t exist';

ok defined $tables->create('fusqlfs_table'), 'Table created';
is_deeply $tables->get('fusqlfs_table'), $tables->{subpackages}, 'New table is sane';
is_deeply $tables->list(), [ 'fusqlfs_table' ], 'New table is listed';

# Rename table
ok defined $tables->rename('fusqlfs_table', 'new_fusqlfs_table'), 'Table renamed';
ok !defined($tables->get('fusqlfs_table')), 'Table is unaccessable under old name';
is_deeply $tables->get('new_fusqlfs_table'), $tables->{subpackages}, 'Table renamed correctly';
is_deeply $tables->list(), [ 'new_fusqlfs_table' ], 'Table is listed under new name';

# Drop table
ok defined $tables->drop('new_fusqlfs_table'), 'Table dropped';
ok !defined($tables->get('new_fusqlfs_table')), 'Table dropped correctly';
is_deeply $tables->list(), [], 'Tables list is empty';

=end testing
=cut

use FusqlFS::Backend::PgSQL::Roles;
use FusqlFS::Backend::PgSQL::Table::Indices;
use FusqlFS::Backend::PgSQL::Table::Struct;
use FusqlFS::Backend::PgSQL::Table::Data;
use FusqlFS::Backend::PgSQL::Table::Constraints;

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
        indices     => new FusqlFS::Backend::PgSQL::Table::Indices(),
        struct      => new FusqlFS::Backend::PgSQL::Table::Struct(),
        data        => new FusqlFS::Backend::PgSQL::Table::Data(),
        constraints => new FusqlFS::Backend::PgSQL::Table::Constraints(),
        owner       => new FusqlFS::Backend::PgSQL::Role::Owner('r', 2),
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

