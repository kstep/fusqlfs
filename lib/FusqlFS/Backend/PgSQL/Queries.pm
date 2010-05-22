use strict;
use v5.10.0;

use FusqlFS::Interface;

package FusqlFS::Backend::PgSQL::Queries;
use base 'FusqlFS::Interface';

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
        return '' unless $query;
        return $self->dump($self->all_row($query));
    };
}

sub drop
{
    my $self = shift;
    my ($name) = @_;
    delete $self->{$name};
}

1;

