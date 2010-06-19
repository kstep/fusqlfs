use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Role::Acl;
use parent 'FusqlFS::Artifact';

=head1 NAME

FusqlFS::Backend::PgSQL::Role::Acl - FusqlFS class to expose PostgreSQL
artifact's permissions

=head1 SYNOPSIS

    package FusqlFS::Backend::PgSQL::Tables;
    use parent 'FusqlFS::Artifact';

    use FusqlFS::Backend::PgSQL::Role::Acl;

    sub new
    {
        my $class = shift;
        my $self = {};

        # initialize class

        $self->{acl} = FusqlFS::Backend::PgSQL::Role::Acl->new('r');
        bless $self, $class;
    }

    sub get
    {
        my $self = shift;
        my ($name) = @_;
        my $result = {};

        # load structures into $result

        $result->{acl} = $self->{acl};
        return $result;
    }

=head1 DESCRIPTION

This class exposes PostgreSQL artifact's permissions (a.k.a. ACL) as a
directory with subdirectories named after roles with marker files named after
permissions. It is best used with plugged in L<FusqlFS::Backend::PgSQL::Roles>
module (see L<FusqlFS::Backend::Base> for more info on plugging in different
modules).

The class's C<new> constructor accepts single char argument designating
type of artifact the owner of which is to be exposed. Possible values are
the same as for L<FusqlFS::Backend::PgSQL::Role::Owner> module.

=head1 EXPOSED STRUCTURE

First level of exposed files are subdirectories named after roles, e.g. if a
table has perms granted to roles C<user1> and C<user2> this module will expose
subdirectories F<./user1> and F<./user2>.

Removing such subdirectory revokes all permissions from the role, creating
subdirectory with some role's name grants all permission to the role.

Every such subdirectory has following structure:

=over

=item F<./granter>

Symlink to role in F<../../../../roles> which granted current role its permissions.

=item F<./role>

Symlink to current role in F<../../../../roles> (i.e. the role with the name
equal to current subdirectory's name).

=item F<./insert>, F<./update>, F<./delete>, F<./references>, F<./trigger>, F<./usage>

Plain files to designated correspondent permission is granted. Remove some of
the files to revoke the permission or create new file with one of the names
(e.g. with C<touch ./insert>) to grant such permission.

=back

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
    execute    X
    usage      U
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
    $name = $1 if $name =~ /^([a-zA-Z0-9_]+)/;
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
    $name = $1 if $name =~ /^([a-zA-Z0-9_]+)/;
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

    my $acl = $self->all_col($self->{get_expr}, $name =~ /([a-zA-Z0-9_]+)/? $1: $name);
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

