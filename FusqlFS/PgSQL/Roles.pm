use strict;
use v5.10.0;
use FusqlFS::Base;

package FusqlFS::PgSQL::Role::Permissions;
use base 'FusqlFS::Base::Interface';

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

package FusqlFS::PgSQL::Role::Owner;
use base 'FusqlFS::Base::Interface';

our %relkinds = qw(
    r TABLE
    i INDEX
    S SEQUENCE
);

sub new
{
    my $class = shift;
    my $relkind = shift;
    my $depth = 0+shift;
    my $self = {};

    $self->{depth} = '../' x $depth;
    $self->{get_expr} = $class->expr("SELECT pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class WHERE relname = ? AND relkind = '$relkind'");
    $self->{store_expr} = "ALTER $relkinds{$relkind} \"%s\" OWNER TO \"%s\"";

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my $name = pop;
    my $owner = $self->all_col($self->{get_expr}, $name);
    return \"$self->{depth}roles/$owner->[0]" if $owner;
}

sub store
{
    my $self = shift;
    my $data = pop;
    my $name = pop;
    $data = $$data if ref $data eq 'SCALAR';
    return if ref $data || $data !~ m#^$self->{depth}roles/([^/]+)$#;
    $self->do($self->{store_expr}, [$name, $1]);
}

1;

package FusqlFS::PgSQL::Role::Owned;
use base 'FusqlFS::Base::Interface';

1;

package FusqlFS::PgSQL::Roles;
use base 'FusqlFS::Base::Interface';
use DBI qw(:sql_types);

sub new
{
    my $class = shift;
    my $self = {};

    $self->{list_expr} = $class->expr("SELECT rolname FROM pg_catalog.pg_roles");
    $self->{get_expr} = $class->expr("SELECT r.rolcanlogin AS can_login, r.rolcatupdate AS cat_update, r.rolconfig AS config,
            r.rolconnlimit AS conn_limit, r.rolcreatedb AS create_db, r.rolcreaterole AS create_role, r.rolinherit AS inherit,
            r.rolsuper AS superuser, r.rolvaliduntil AS valid_until,
            ARRAY(SELECT b.rolname FROM pg_catalog.pg_roles AS b
                    JOIN pg_catalog.pg_auth_members AS m ON (m.member = b.oid)
                WHERE m.roleid = r.oid) AS contains
        FROM pg_catalog.pg_roles AS r WHERE rolname = ?");

    $self->{rename_expr} = 'ALTER ROLE "%s" RENAME TO "%s"';
    $self->{drop_expr} = 'DROP ROLE "%s"';

    $self->{revoke_expr} = 'REVOKE "%s" FROM "%s"';
    $self->{grant_expr} = 'GRANT "%s" TO "%s"';

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($name) = @_;

    my $data = $self->one_row($self->{get_expr}, $name);
    return unless $data;

    my $result = { map { $_ => \"../$_" } @{$data->{contains}} };

    delete $data->{contains};
    $result->{struct} = $self->dump($data);
    return $result;
}

sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr})||[];
}

sub rename
{
    my $self = shift;
    my ($name, $newname) = @_;
    $self->do($self->{rename_expr}, [$name, $newname]);
}

sub drop
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{drop_expr}, [$name]);
}

sub store
{
    my $self = shift;
    my ($name, $data) = @_;

    my $olddata = $self->one_row($self->{get_expr}, $name);
    my %contains = map { $_ => 1 } @{$olddata->{contains}};
    my @revoke = grep { !exists $data->{$_} } @{$olddata->{contains}};
    my @grant = grep { ref $data->{$_} eq 'SCALAR' && !exists $contains{$_} } keys %{$data};

    $self->do($self->{revoke_expr}, [$name, $_]) foreach @revoke;
    $self->do($self->{grant_expr}, [$name, $_]) foreach @grant;

    $data = $self->load($data->{struct})||{};

    my $sql = "ALTER ROLE \"$name\" ";

    my %options = qw(
        superuser   SUPERUSER
        create_db   CREATEDB
        create_role CREATEROLE
        inherit     INHERIT
        can_login   LOGIN
    );

    my %params = (
        conn_limit  => ['CONNECTION LIMIT', SQL_INTEGER],
        valid_until => ['VALID UNTIL', SQL_TIMESTAMP],
        password    => ['PASSWORD', SQL_VARCHAR],
    );

    foreach (keys %options)
    {
        next unless exists $data->{$_};
        $sql .= 'NO' unless $data->{$_};
        $sql .= $options{$_}.' ';
    }

    my @binds;
    my @types;
    foreach (keys %params)
    {
        next unless $data->{$_};
        $sql .= $params{$_}->[0].' ? ';
        push @binds, $data->{$_};
        push @types, $params{$_}->[1];
    }

    my $sth = $self->expr($sql);
    foreach (0..$#binds)
    {
        $sth->bind_param($_+1, $binds[$_], $types[$_]);
    }
    $sth->execute();
}

1;

