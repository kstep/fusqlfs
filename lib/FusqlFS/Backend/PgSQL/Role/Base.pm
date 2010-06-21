use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Role::Base;
use base 'FusqlFS::Artifact';

=head1 NAME

FusqlFS::Backend::PgSQL::Role::Base - abstract PostgreSQL configuration
complexity for FusqlFS::Backend::PgSQL::Role::* classes

=head1 SYNOPSIS

    package FusqlFS::Backend::PgSQL::Role::Acl;
    use parent 'FusqlFS::Backend::PgSQL::Role::Base';

    sub new
    {
        my $class = shift;
        my $relkind = shift;
        my @kind = $class->kind($relkind);
        my $self = {};

        # initialize instance

        bless $self, $class;
    }

=head1 DESCRIPTION

This class hides configuration complexity for
C<FusqlFS::Backend::PgSQL::Role::*> family of modules including
L<FusqlFS::Backend::PgSQL::Role::Acl> and
L<FusqlFS::Backend::PgSQL::Role::Owner>.

Its single method C<kind()> accepts single character designating the kind of
referenced database artifact and returns a number of configuration parameters
to construct correct SQL statements for the artifact's kind.

If list context C<kind()> method returns a list of C<($kind, $pfx, $table,
$kindc)> where

=over

=item C<$kind>

is the name of artifact type, e.g. C<TABLE>, C<INDEX>, C<SEQUENCE>, C<VIEW>,
C<FUNCTION> or C<LANGUAGE>,

=item C<$pfx>

is the prefix for fields in C<pg_catalog> schema's table with data of the
artifact type, e.g. C<rel> for C<pg_class> or C<pro> for C<pg_proc>,

=item C<$table>

is the table in C<pg_catalog> schema with information about artifacts of this
kind, e.g. C<pg_class>, C<pg_proc> or C<pg_language>,

=item C<$kindc>

is the additional C<WHERE> clause for C<pg_class> table to filter data by
required C<relkind> field value, contains empty string for tables other than
C<pg_class>.

=back

In scalar context this method returns hashref with keys named C<kind>, C<pfx>,
C<table>, C<kindc> and values as described above, so this hashref is usable
with C<FusqlFS::Artifact/hprintf> method.

=cut

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

sub kind
{
    my $class = shift;
    my ($relkind) = @_;

    my ($kind, $pfx) = @{$relkinds{$relkind}};
    my $table = $reltables{$pfx};
    my $kindclause = $table eq 'pg_class'? "AND relkind = '$relkind'": "";

    return wantarray? ($kind, $pfx, $table, $kindclause): { kind => $kind, pfx => $pfx, table => $table, kindc => $kindclause };
}

1;
