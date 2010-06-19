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
    $self->{create_expr} = "GRANT ALL ON $kind %s TO %s";
    $self->{drop_expr}   = "REVOKE ALL ON $kind %s FROM %s";

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my $role = pop;
    my $name = pop;
    my $acl = $self->all_col($self->{get_expr}, $name);
    return unless $acl && @$acl;
    my @acl = split /[=\/]/, (grep /^$role=/, @{$acl->[0]})[0];
    return unless @acl;

    return { granter => \"roles/$acl[2]", role => \"roles/$acl[0]", map { $_ => 1 } grep $acl[1] =~ /$aclmap{$_}/, @{$self->{perms}} };
}

sub list
{
    my $self = shift;
    my $name = pop;
    my $acl = $self->all_col($self->{get_expr}, $name);
    return unless $acl && @$acl;
    return [ map { (split(/=/, $_))[0] } @{$acl->[0]} ];
}

sub store
{
    my $self = shift;
    my $perms = $self->validate(pop, { map { '-'.$_ => undef } @{$self->{perms}} }) or return;
    my @newacl = keys %$perms;
    my $role = pop;
    my $name = pop;

    my $acl = $self->all_col($self->{get_expr}, $name);
    return unless $acl && @$acl;
    my $oldacl = (split /[=\/]/, (grep /^$role=/, @{$acl->[0]})[0])[1];
    return unless $oldacl;
    my @oldacl = grep $oldacl =~ /$aclmap{$_}/, @{$self->{perms}};

    my ($grant, $revoke) = $self->adiff(\@oldacl, \@newacl);
    $self->do($self->{revoke_expr}, [$_, $name, $role]) foreach @$revoke;
    $self->do($self->{grant_expr},  [$_, $name, $role]) foreach @$grant;
}

sub create
{
    my $self = shift;
    my $role = pop;
    my $name = pop;
    $self->do($self->{create_expr}, [$name, $role]);
}

sub drop
{
    my $self = shift;
    my $role = pop;
    my $name = pop;
    $self->do($self->{drop_expr}, [$name, $role]);
}

1;

