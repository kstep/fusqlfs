use strict;
use v5.10.0;

use FusqlFS::Base;

package FusqlFS::PgSQL::Queries;
use base 'FusqlFS::Base::Interface';

sub new
{
    my $class = shift;
    my $self = {};
    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($name) = @_;
    return $self->{$name}||undef;
}

sub list
{
    my $self = shift;
    return [ keys %$self ];
}

sub create
{
    my $self = shift;
    my ($name) = @_;
    $self->{$name} = sub () {
        my $query = shift;
        my $expr = $self->expr($query);
        return $self->dump($self->all_row($expr));
    };
}

sub drop
{
    my $self = shift;
    my ($name) = @_;
    delete $self->{$name};
}

1;

