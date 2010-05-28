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

    $self->{create_expr} = 'CREATE OR REPLACE FUNCTION %s(integer) RETURNS integer LANGUAGE sql AS $function$ SELECT $1; $function$';

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
    $result->{'content.'.$data->{lang}} = $data->{content};

    delete $data->{content};
    delete $data->{lang};
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
