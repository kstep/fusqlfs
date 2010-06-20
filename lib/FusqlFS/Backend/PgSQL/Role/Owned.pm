use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Role::Owned;
use base 'FusqlFS::Artifact';

=head1 NAME

FusqlFS::Backend::PgSQL::Role::Owned - FusqlFS module to aggregate all
PostgreSQL artifacts owned by a role into single place

=head1 SYNOPSIS

    package FusqlFS::Backend::PgSQL::Roles;
    use base 'FusqlFS::Artifact';

    use FusqlFS::Backend::PgSQL::Role::Owned;

    sub new
    {
        my $class = shift;
        my $self = {};

        # initialize instance and class

        $self->{owned} = FusqlFS::Backend::PgSQL::Role::Owned->new();

        bless $self, $class;
    }

    sub get
    {
        my $class = shift;
        my ($role) = @_;
        my $result = {};

        # get data about $role into $result

        $result->{owned} = $self->{owned};

        return $result;
    }

=head1 DESCRIPTION

This module gathers all database artifacts owned by single role into one single
place. It does it by providing symlinks to artifacts sorted by type (`tables',
`functions' etc., see L</EXPOSED STRUCTURE> for details).

This module is not to be plugged in any place of tree, it is actually to be
used in L</FusqlFS::Backend::PgSQL::Roles> module.

=head1 EXPOSED STRUCTURE

=over

=item F<./tables>, F<./functions>, F<./indices>, F<./sequences>, F<./languages>

This is the first level of hierarchy to sort owned artifacts by type and
provide separate namespaces for different objects in database.

There are symlinks to artifacts of correspondent type in each of these
subdirectories, e.g. if a role owns table C<sometable> and stored procedure
C<someproc> there will be symlinks in F<./tables/sometable> and
F<./functions/someproc> pointing to F</tables/sometable> and
F</functions/someproc>.

=back

=cut

sub new
{
    my $class = shift;
    my $self = {};

    #Body

    bless $self, $class;
}

1;
