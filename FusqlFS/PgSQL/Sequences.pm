use strict;
use v5.10.0;
use FusqlFS::Base;

package FusqlFS::PgSQL::Sequences;
use base 'FusqlFS::Base::Interface';

sub new
{
    my $class = shift;
    my $self = {};

    $self->{list_expr} = $class->expr("SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'S'");
    $self->{get_expr} = $class->expr("SELECT 1 FROM pg_catalog.pg_class WHERE relname = ? AND relkind = 'S'");

    $self->{subpackages} = {
    };

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($name) = @_;
    my $result = $self->all_col($self->{get_expr}, $name);
    return $self->{subpackages} if @$result;
}

sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr}) || [];
}

1;

