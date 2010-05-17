use strict;
use v5.10.0;

use FusqlFS::Base;

package FusqlFS::PgSQL::Query;
use base 'FusqlFS::Base::Interface';

sub new
{
    my $class = shift;
    my $self = [$class->expr($_[0])];

    bless $self, $class;
}

sub get
{
    my $self = shift;
    return $self->dump($self->all_row($self->[0]));
}

1;

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
    $self->{$name} = { 'query.sql' => '' };
}

sub drop
{
    my $self = shift;
    my ($name) = @_;
    delete $self->{$name};
}

sub store
{
    my $self = shift;
    my ($name, $data) = @_;
    $self->{$name} = { 'query.sql' => $data->{'query.sql'}, 'data' => new FusqlFS::PgSQL::Query($data->{'query.sql'}) };
}

1;

