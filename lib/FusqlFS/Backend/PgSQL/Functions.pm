use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Functions;
use parent 'FusqlFS::Artifact';

use FusqlFS::Backend::PgSQL::Roles;

sub new
{
    my $class = shift;
    my $self = {};

    my $pgver = $class->dbh->{pg_server_version};
    my $get_func_args = 'pg_catalog.pg_get_function_arguments(p.oid)';
    my $get_func_res  = 'pg_catalog.pg_get_function_result(p.oid)';

    if ($pgver < 80400)
    {
        $get_func_args = 'CASE WHEN p.proallargtypes IS NOT NULL THEN
    pg_catalog.array_to_string(ARRAY(
      SELECT
        CASE
          WHEN p.proargmodes[s.i] = \'i\' THEN \'\'
          WHEN p.proargmodes[s.i] = \'o\' THEN \'OUT \'
          WHEN p.proargmodes[s.i] = \'b\' THEN \'INOUT \'
          WHEN p.proargmodes[s.i] = \'v\' THEN \'VARIADIC \'
        END ||
        CASE
          WHEN COALESCE(p.proargnames[s.i], \'\') = \'\' THEN \'\'
          ELSE p.proargnames[s.i] || \' \' 
        END ||
        pg_catalog.format_type(p.proallargtypes[s.i], NULL)
      FROM
        pg_catalog.generate_series(1, pg_catalog.array_upper(p.proallargtypes, 1)) AS s(i)
    ), \', \')
  ELSE
    pg_catalog.array_to_string(ARRAY(
      SELECT
        CASE
          WHEN COALESCE(p.proargnames[s.i+1], \'\') = \'\' THEN \'\'
          ELSE p.proargnames[s.i+1] || \' \'
          END ||
        pg_catalog.format_type(p.proargtypes[s.i], NULL)
      FROM
        pg_catalog.generate_series(0, pg_catalog.array_upper(p.proargtypes, 1)) AS s(i)
    ), \', \')
  END';
        $get_func_res = 'pg_catalog.format_type(p.prorettype, NULL)';
    }

    $self->{list_expr} = $class->expr("SELECT DISTINCT p.proname||'('||$get_func_args||')' FROM pg_catalog.pg_proc AS p
                LEFT JOIN pg_catalog.pg_namespace AS ns ON ns.oid = p.pronamespace
            WHERE ns.nspname = 'public'");

    #CASE WHEN p.proisagg THEN NULL ELSE pg_catalog.pg_get_functiondef(p.oid) END AS struct
    $self->{get_expr} = $class->expr("SELECT $get_func_res AS result,
                $get_func_args AS arguments,
                trim(both from p.prosrc) AS content, l.lanname AS lang,
                CASE p.provolatile WHEN 'i' THEN 'immutable' WHEN 's' THEN 'stable' WHEN 'v' THEN 'volatile' END AS volatility,
                CASE WHEN p.proisagg THEN 'aggregate' WHEN p.proiswindow THEN 'window' WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger' ELSE NULL END AS type
            FROM pg_catalog.pg_proc AS p
                LEFT JOIN pg_catalog.pg_language AS l ON l.oid = p.prolang
            WHERE p.proname = ? AND $get_func_args = ?");

    $self->{create_expr} = 'CREATE OR REPLACE FUNCTION %s RETURNS integer LANGUAGE sql AS $function$ SELECT 1; $function$';
    $self->{rename_expr} = 'ALTER FUNCTION %s RENAME TO %s';
    $self->{drop_expr} = 'DROP FUNCTION %s';
    $self->{store_expr} = 'CREATE OR REPLACE FUNCTION %s RETURNS %s LANGUAGE %s AS $function$ %s $function$ %s';

    $self->{owner} = new FusqlFS::Backend::PgSQL::Role::Owner('_F', 2);

    bless $self, $class;
}

=begin testing get

my $row = $_tobj->get('xxxxx');
is $row, undef, '->get() result is sane';

=end testing
=cut
sub get
{
    my $self = shift;
    my ($name, $args) = split(/\(/, $_[0], 2);
    return unless $args;
    $args =~ s/\)$//;
    my $data = $self->one_row($self->{get_expr}, $name, $args);
    return unless $data;

    my $result = {};
    $result->{language} = \"../../languages/$data->{lang}";
    $result->{'content.'.delete($data->{lang})} = delete($data->{content});
    $result->{struct} = $self->dump($data);

    $result->{owner} = $self->{owner};

    return $result;
}

=begin testing list

my $list = $_tobj->list();
isa_ok $list, 'ARRAY', '->list() result is an array';
cmp_ok scalar(@$list), '==', 0, '->list() result is empty';

=end testing
=cut
sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr});
}

=begin testing drop after store

is $_tobj->drop('fusqlfs_func(integer)'), undef;
isnt $_tobj->drop('fusqlfs_func(integer, integer)'), undef;
is_deeply $_tobj->list(), [];
is $_tobj->get('fusqlfs_func(integer, integer)'), undef;

=end testing
=cut
sub drop
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{drop_expr}, [$name]);
}

=begin testing store after rename

isnt $_tobj->store('fusqlfs_func(integer, integer)', $new_func), undef;
is_deeply $_tobj->get('fusqlfs_func(integer, integer)'), $new_func;

=end testing
=cut
sub store
{
    my $self = shift;
    my ($name, $data) = @_;
    my $struct = $self->load($data->{struct}||{});
    return unless ref($struct) eq 'HASH';

    my ($contkey) = grep(/^content\./, keys %$data);
    return unless $contkey;

    my $content = $data->{$contkey};
    my (undef, $lang) = split /\./, $contkey, 2;
    return unless $content && $lang;

    my $opts = ' ';
    $opts .= 'WINDOW ' if defined($struct->{type}) && $struct->{type} eq 'window';
    $opts .= uc $struct->{volatility} if defined($struct->{volatility})
        and grep $struct->{volatility} eq $_, qw(volatile immutable stable);

    $self->do($self->{store_expr}, [$name, $struct->{result}, $lang, $content, $opts]);
}

=begin testing rename after create

isnt $_tobj->rename('fusqlfs_func(integer)', 'fusqlfs_func(integer, integer)'), undef;
$created_func->{struct} =~ s/^arguments: integer$/arguments: 'integer, integer'/m;
is_deeply $_tobj->get('fusqlfs_func(integer, integer)', $created_func), $created_func;
is $_tobj->get('fusqlfs_func(integer)'), undef;
is_deeply $_tobj->list(), [ 'fusqlfs_func(integer, integer)' ];

=end testing
=cut
sub rename
{
    my $self = shift;
    my ($name, $newname) = @_;
    return if $name eq $newname;

    my ($fname, $fargs) = split /\(/, $newname, 2;
    my ($aname, $aargs) = split /\(/, $name, 2;
    if ($fargs eq $aargs)
    {
        $self->do($self->{rename_expr}, [$name, $fname]);
    }
    else
    {
        my $data = $self->get($name);
        $self->drop($name);
        $self->store($newname, $data);
    }
}

=begin testing create after get list

isnt $_tobj->create('fusqlfs_func(integer)'), undef;
is_deeply $_tobj->list(), [ 'fusqlfs_func(integer)' ];
is_deeply $_tobj->get('fusqlfs_func(integer)'), $created_func;

=end testing
=cut
sub create
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{create_expr}, [$name]);
}

1;

__END__

=begin testing SETUP

#!class FusqlFS::Backend::PgSQL::Test

my $created_func = {
    'content.sql' => 'SELECT 1;',
    'language' => \'../../languages/sql',
    'struct' => '---
arguments: integer
result: integer
type: ~
volatility: volatile
',
    'owner' => $_tobj->{owner},
};

my $new_func = {
    'content.sql' => 'SELECT $1 | $2;',
    'language' => \'../../languages/sql',
    'struct' => '---
arguments: \'integer, integer\'
result: integer
type: ~
volatility: immutable
',
    'owner' => $_tobj->{owner},
};

=end testing
=cut
