use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Role::Owner;

=head1 NAME

FusqlFS::Backend::PgSQL::Role::Owner - FusqlFS class to expose artifact's owner

=head1 SYNOPSIS


=head1 DESCRIPTION


=cut

use parent 'FusqlFS::Artifact';

our %relkinds = (
    r  => [ qw(TABLE rel) ],
    i  => [ qw(INDEX rel) ],
    S  => [ qw(SEQUENCE rel) ],
    v  => [ qw(VIEW rel) ],

    _F => [ qw(FUNCTION pro) ],
    _L => [ qw(LANGUAGE lan) ],
);

our %reltables = qw(
    rel pg_class
    pro pg_proc
    lan pg_language
);

sub new
{
    my $class = shift;
    my $relkind = shift;

    my ($kind, $rel) = @{$relkinds{$relkind}};
    my $table = $reltables{$rel};
    my $kindclause = $table eq 'pg_class'? "AND relkind = '$relkind'": "";

    my $self = {};

    $self->{get_expr} = $class->expr("SELECT pg_catalog.pg_get_userbyid(${rel}owner) FROM pg_catalog.$table WHERE ${rel}name = ? $kindclause");
    $self->{store_expr} = "ALTER ${kind} \"%s\" OWNER TO \"%s\"";

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my $name = pop;
    $name = $1 if $name =~ /^([a-zA-Z0-9_]+)/;
    my $owner = $self->all_col($self->{get_expr}, $name);
    return \"roles/$owner->[0]" if $owner;
}

sub store
{
    my $self = shift;
    my $data = pop;
    my $name = pop;
    $data = $$data if ref $data eq 'SCALAR';
    return if ref $data || $data !~ m#^roles/([^/]+)$#;
    $self->do($self->{store_expr}, [$name, $1]);
}

1;
