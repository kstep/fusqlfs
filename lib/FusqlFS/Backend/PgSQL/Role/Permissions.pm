use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Role::Permissions;
use parent 'FusqlFS::Artifact';

=head1 NAME

FusqlFS::Backend::PgSQL::Role::Permissions - FusqlFS class to expose PostgreSQL
artifact's permissions

=head1 SYNOPSIS


=head1 DESCRIPTION


=head1 METHODS

=over

=cut

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

