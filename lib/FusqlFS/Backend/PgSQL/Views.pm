use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Views;
use parent 'FusqlFS::Artifact';
use FusqlFS::Backend::PgSQL::Roles;

sub new
{
    my $class = shift;
    my $self = {};

    $self->{drop_expr} = 'DROP VIEW "%s"';
    $self->{create_expr} = 'CREATE VIEW "%s" AS SELECT 1';
    $self->{store_expr} = 'CREATE OR REPLACE VIEW "%s" AS %s';
    $self->{rename_expr} = 'ALTER VIEW "%s" RENAME TO "%s"';

    $self->{get_expr} = $class->expr("SELECT definition FROM pg_catalog.pg_views WHERE viewname = ?");
    $self->{list_expr} = $class->expr("SELECT viewname FROM pg_catalog.pg_views WHERE schemaname = 'public'");

    $self->{owner} = new FusqlFS::Backend::PgSQL::Role::Owner('v', 2);

    bless $self, $class;
}

=begin testing list

list_ok $_tobj->list(), [];

=end testing
=cut
sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr});
}

=begin testing get

is $_tobj->get('unknown'), undef;

=end testing
=cut
sub get
{
    my $self = shift;
    my ($name) = @_;
    my $result = $self->all_col($self->{get_expr}, $name);
    return unless @$result;
    return {
        'query.sql' => $result->[0],
        owner => $self->{owner},
    };
}

=begin testing rename after store

isnt $_tobj->rename('fusqlfs_view', 'new_fusqlfs_view'), undef;
is $_tobj->get('fusqlfs_view'), undef;
is_deeply $_tobj->get('new_fusqlfs_view'), { 'query.sql' => 'SELECT 2;', owner => $_tobj->{owner} };
is_deeply $_tobj->list(), [ 'new_fusqlfs_view' ];

=end testing
=cut
sub rename
{
    my $self = shift;
    my ($name, $newname) = @_;
    $self->do($self->{'rename_expr'}, [$name, $newname]);
}

=begin testing drop after rename

isnt $_tobj->drop('new_fusqlfs_view'), undef;
is_deeply $_tobj->list(), [];
is $_tobj->get('new_fusqlfs_view'), undef;

=end testing
=cut
sub drop
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{'drop_expr'}, [$name]);
}

=begin testing create after get list

isnt $_tobj->create('fusqlfs_view'), undef;
is_deeply $_tobj->list(), [ 'fusqlfs_view' ];
is_deeply $_tobj->get('fusqlfs_view'), { 'query.sql' => 'SELECT 1;', owner => $_tobj->{owner} };

=end testing
=cut
sub create
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{'create_expr'}, [$name]);
}

=begin testing store after create

isnt $_tobj->store('fusqlfs_view', { 'query.sql' => 'SELECT 2' }), undef;
is_deeply $_tobj->get('fusqlfs_view'), { 'query.sql' => 'SELECT 2;', owner => $_tobj->{owner} };

=end testing
=cut
sub store
{
    my $self = shift;
    my ($name, $data) = @_;
    $self->do($self->{'store_expr'}, [$name, $data->{'query.sql'}]);
}

1;

__END__

=begin testing SETUP

#!class FusqlFS::Backend::PgSQL::Test

=end testing
