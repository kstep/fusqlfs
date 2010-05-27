use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Functions;
use parent 'FusqlFS::Artifact';

sub new
{
    my $class = shift;
    my $self = {};

    $self->{list_expr} = $class->expr('SELECT DISTINCT p.proname FROM pg_catalog.pg_proc AS p
                LEFT JOIN pg_catalog.pg_namespace AS ns ON ns.oid = p.pronamespace
            WHERE ns.nspname = \'public\'');

    $self->{get_expr} = $class->expr('SELECT pg_catalog.pg_get_function_result(p.oid) AS result,
                pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
                CASE WHEN p.proisagg THEN NULL ELSE pg_catalog.pg_get_functiondef(p.oid) END AS struct
            FROM pg_catalog.pg_proc AS p WHERE p.proname = ? ORDER BY arguments, result');

    $self->{create_expr} = 'CREATE OR REPLACE FUNCTION %s(integer) RETURNS integer LANGUAGE sql AS $function$ SELECT $1; $function$';

    bless $self, $class;
}

=begin testing get

my $row = $_tobj->get('sort');
isnt $row, undef, '->get() result is sane';

=end testing
=cut
sub get
{
    my $self = shift;
    my ($name) = @_;
    return $self->all_row($self->{get_expr}, $name);
}

=begin testing list

my $list = $_tobj->list();
isa_ok $list, 'ARRAY', '->list() result is an array';
cmp_ok scalar(@$list), '>', 0, '->list() result is not empty';

=end testing
=cut
sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr});
}

sub store
{
    my $self = shift;
    my ($name, $data) = @_;
    my $sql = $data->{struct}||"";
    return unless $sql;

    $self->do($sql);
}

1;

__END__

=begin testing SETUP

#!class FusqlFS::Backend::PgSQL::Test

=end testing
=cut
