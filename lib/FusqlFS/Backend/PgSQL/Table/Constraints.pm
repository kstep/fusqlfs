use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Table::Constraints;
use parent 'FusqlFS::Artifact';

sub new
{
    my $class = shift;
    my $self = {};

    $self->{get_expr} = $class->expr('SELECT pg_catalog.pg_get_constraintdef(co.oid, true) AS struct, co.contype AS ".type" FROM pg_catalog.pg_constraint co
            JOIN pg_catalog.pg_class AS cl ON (cl.oid = co.conrelid) WHERE cl.relname = ? AND co.conname = ?');
    $self->{list_expr} = $class->expr('SELECT co.conname FROM pg_catalog.pg_constraint AS co
            JOIN pg_catalog.pg_class AS cl ON (cl.oid = co.conrelid) WHERE cl.relname = ?');

    bless $self, $class;
}

=begin testing store after create

TODO: {
local $TODO = 'PgSQL::Constraints mutation unimlemented';

isnt $_tobj->store('fusqlfs_table', 'fusqlfs_constraint', $new_constraint), undef;
is_deeply $_tobj->get('fusqlfs_table', 'fusqlfs_constraint'), $new_constraint;
}

=end testing
=cut
sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;
}

=begin testing rename after store

TODO: {
local $TODO = 'PgSQL::Constraints mutation unimlemented';

isnt $_tobj->rename('fusqlfs_table', 'fusqlfs_constraint', 'new_fusqlfs_constraint'), undef;
is $_tobj->get('fusqlfs_table', 'fusqlfs_constraint'), undef;
is_deeply $_tobj->get('fusqlfs_table', 'new_fusqlfs_constraint'), $new_constraint;
is_deeply [ sort(@{$_tobj->list('fusqlfs_table')}) ], [ sort('fusqlfs_table_pkey', 'new_fusqlfs_constraint') ];
}

=end testing
=cut
sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
}

=begin testing create after get list

TODO: {
local $TODO = 'PgSQL::Constraints mutation unimlemented';

isnt $_tobj->create('fusqlfs_table', 'fusqlfs_constraint'), undef;
is_deeply $_tobj->get('fusqlfs_table', 'fusqlfs_constraint'), {};
is_deeply [ sort(@{$_tobj->list('fusqlfs_table')}) ], [ sort('fusqlfs_table_pkey', 'fusqlfs_constraint') ];
}

=end testing
=cut
sub create
{
    my $self = shift;
    my ($table, $name) = @_;
}

=begin testing drop after rename

TODO: {
local $TODO = 'PgSQL::Constraints mutation unimlemented';

isnt $_tobj->drop('fusqlfs_table', 'new_fusqlfs_constraint'), undef;
is $_tobj->get('fusqlfs_table', 'new_fusqlfs_constraint'), undef;
is_deeply $_tobj->list('fusqlfs_table'), [ 'fusqlfs_table_pkey' ];
}

=end testing
=cut
sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
}

=begin testing get

is $_tobj->get('fusqlfs_table', 'unknown'), undef;
is_deeply $_tobj->get('fusqlfs_table', 'fusqlfs_table_pkey'), { struct => 'PRIMARY KEY (id)', '.type' => 'p' };

=end testing
=cut
sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $data = $self->one_row($self->{get_expr}, $table, $name);
    return unless $data;
    if ($data->{".type"} eq 'f')
    {
        my ($myfields, $table, $herfields) = ($data->{struct} =~ /KEY \((.+?)\) REFERENCES (.+?)\((.+?)\)/);
        my @myfields = split /,/, $myfields;
        my @herfields = split /,/, $herfields;
        foreach (0..$#myfields)
        {
            $data->{$myfields[$_]} = \"../../../$table/struct/$herfields[$_]";
        }
    }
    return $data;
}

=begin testing list

#is $_tobj->list('unknown'), undef;
list_ok $_tobj->list('fusqlfs_table'), [ 'fusqlfs_table_pkey' ];

=end testing
=cut
sub list
{
    my $self = shift;
    my ($table) = @_;
    return $self->all_col($self->{list_expr}, $table);
}

1;

__END__

=begin testing SETUP

#!class FusqlFS::Backend::PgSQL::Table::Test

my $new_constraint = {
};

=end testing
=cut
