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

use FusqlFS::Backend::PgSQL::Role::Owner;

our %relperms = (
    r  => [ qw(select insert update delete references trigger) ],
    S  => [ qw(usage select update) ],
    _F => [ qw(execute) ],
    _L => [ qw(usage) ],
);

our %aclmap = qw(
    select     r
    insert     a
    update     w
    delete     d
    references x
    trigger    t
    execute    x
    usage      u
);

sub new
{
    my $class = shift;
    my $relkind = shift;
    my $self = {};

    my ($kind, $rel) = @{$FusqlFS::Backend::PgSQL::Role::Owner::relkinds{$relkind}};
    my $table = $FusqlFS::Backend::PgSQL::Role::Owner::reltables{$rel};
    my $kindclause = $table eq 'pg_class'? "AND relkind = '$relkind'": "";

    $self->{perms} = $relperms{$relkind};
    $self->{get_expr} = $class->expr("SELECT ${rel}acl FROM pg_catalog.$table WHERE ${rel}name = ? $kindclause");
    $self->{grant_expr}  = "GRANT %s ON $kind %s TO %s";
    $self->{revoke_expr} = "REVOKE %s ON $kind %s FROM %s";

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my $perm = pop;
    my $name = pop;
    my $acl = $self->all_col($self->{get_expr}, $name);
    return unless $acl && @$acl;
    my %acl = map { (split(/[=\/]/, $_))[0,1] } @{$acl->[0]};

    return { map { $_ => \"roles/$_" } grep $acl{$_} =~ /$aclmap{$perm}/, keys %acl };
}

sub list
{
    my $self = shift;
    return $self->{perms};
}

sub store
{
    my $self = shift;
    my $roles = $self->validate(pop, ['HASH', sub{ [ keys %$_ ] }]) or return;
    my $perm = pop;
    my $name = pop;

    my @oldroles = keys %{$self->get($name, $perm)};
    my ($grant, $revoke) = $self->adiff(\@oldroles, $roles);
    $self->do($self->{revoke_expr}, [$perm, $name, $_]) foreach @$revoke;
    $self->do($self->{grant_expr},  [$perm, $name, $_]) foreach @$grant;
}

1;

