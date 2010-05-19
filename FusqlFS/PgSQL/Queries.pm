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
        state $expr;
        my $query = shift;
        return unless $expr || $query;
        if ($query)
        {
            $expr = $self->expr($query);
            $expr->execute;
        }

        return $self->dump($expr->fetchrow_hashref);
    };
}

sub drop
{
    my $self = shift;
    my ($name) = @_;
    delete $self->{$name};
}

1;

