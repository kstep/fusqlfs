use strict;
use v5.10.0;
use FusqlFS::Base;

package FusqlFS::PgSQL::Role::Permissions;
use base 'FusqlFS::Base::Interface';

sub get
{
    my $self = shift;
    my ($name) = @_;
    return {
        tables    => {},
        views     => {},
        functions => {},
    };
}

sub list
{
    return [ qw(tables views functions) ];
}

1;

package FusqlFS::PgSQL::Role::Owner;
use base 'FusqlFS::Base::Interface';

our %relkinds = qw(
    r TABLE
    i INDEX
    S SEQUENCE
);

sub new
{
    my $class = shift;
    my $relkind = shift;
    my $depth = 0+shift;
    my $self = {};

    $self->{depth} = '../' x $depth;
    $self->{get_expr} = $class->expr("SELECT pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class WHERE relname = ? AND relkind = '$relkind'");
    $self->{store_expr} = "ALTER $relkinds{$relkind} \"%s\" OWNER TO \"%s\"";

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my $name = pop;
    my $owner = $self->all_col($self->{get_expr}, $name);
    return \"$self->{depth}roles/$owner->[0]" if $owner;
}

sub store
{
    my $self = shift;
    my $data = pop;
    my $name = pop;
    $data = $$data if ref $data eq 'SCALAR';
    return if ref $data || $data !~ m#^$self->{depth}roles/([^/]+)$#;
    $self->do($self->{store_expr}, [$name, $1]);
}

1;

package FusqlFS::PgSQL::Role::Owned;
use base 'FusqlFS::Base::Interface';

1;

package FusqlFS::PgSQL::Roles;
use base 'FusqlFS::Base::Interface';

sub new
{
    my $class = shift;
    my $self = {};

    $self->{'list_expr'} = $class->expr("SELECT rolname FROM pg_catalog.pg_roles");
    $self->{'get_expr'} = $class->expr("SELECT * FROM pg_catalog.pg_roles WHERE rolname = ?");

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($name) = @_;
    return $self->dump($self->one_row($self->{'get_expr'}, $name));
}

sub list
{
    my $self = shift;
    return $self->all_col($self->{'list_expr'})||[];
}

1;

