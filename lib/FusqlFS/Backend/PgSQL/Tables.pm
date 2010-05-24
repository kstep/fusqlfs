use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Tables;
use parent 'FusqlFS::Artifact';

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

=begin testing get

ok !defined($testclass->get('fusqlfs_table')), 'Test table doesn\'t exist';

=end testing
=cut
sub get
{
    my $self = shift;
    my ($name) = @_;
    my $result = $self->all_col($self->{get_expr}, $name);
    return unless @$result;
    return $self->{subpackages};
}

=begin testing drop after rename

ok defined $testclass->drop('new_fusqlfs_table'), 'Table dropped';
ok !defined($testclass->get('new_fusqlfs_table')), 'Table dropped correctly';
is_deeply $testclass->list(), [], 'Tables list is empty';

=end testing
=cut
sub drop
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{drop_expr}, [$name]);
}

=begin testing create after get list

ok defined $testclass->create('fusqlfs_table'), 'Table created';
is_deeply $testclass->get('fusqlfs_table'), $testclass->{subpackages}, 'New table is sane';
is_deeply $testclass->list(), [ 'fusqlfs_table' ], 'New table is listed';

=end testing
=cut
sub create
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{create_expr}, [$name]);
}

=begin testing rename after create

ok defined $testclass->rename('fusqlfs_table', 'new_fusqlfs_table'), 'Table renamed';
ok !defined($testclass->get('fusqlfs_table')), 'Table is unaccessable under old name';
is_deeply $testclass->get('new_fusqlfs_table'), $testclass->{subpackages}, 'Table renamed correctly';
is_deeply $testclass->list(), [ 'new_fusqlfs_table' ], 'Table is listed under new name';

=end testing
=cut
sub rename
{
    my $self = shift;
    my ($name, $newname) = @_;
    $self->do($self->{rename_expr}, [$name, $newname]);
}

=begin testing list

list_ok $testclass->list(), [], 'Tables list is sane';

=end testing
=cut
sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr}) || [];
}

1;

__END__

=begin testing SETUP

#!class FusqlFS::Backend::PgSQL::Test

=end testing
