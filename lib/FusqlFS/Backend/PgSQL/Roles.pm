use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Role::Permissions;
use parent 'FusqlFS::Artifact';

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

package FusqlFS::Backend::PgSQL::Role::Owner;
use parent 'FusqlFS::Artifact';

our %relkinds = (
    r  => [ qw(TABLE rel) ],
    i  => [ qw(INDEX rel) ],
    S  => [ qw(SEQUENCE rel) ],
    v  => [ qw(VIEW rel) ],

    _F => [ qw(FUNCTION pro) ],
);

our %reltables = qw(
    rel pg_class
    pro pg_proc
);

sub new
{
    my $class = shift;
    my $relkind = shift;
    my $depth = 0+shift;

    my ($kind, $rel) = @{$relkinds{$relkind}};
    my $table = $reltables{$rel};
    my $kindclause = $table eq 'pg_class'? "AND relkind = '$relkind'": "";

    my $self = {};

    $self->{depth} = '../' x $depth;
    $self->{get_expr} = $class->expr("SELECT pg_catalog.pg_get_userbyid(${rel}owner) FROM pg_catalog.$table WHERE ${rel}name = ? $kindclause");
    $self->{store_expr} = "ALTER ${kind} \"%s\" OWNER TO \"%s\"";

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my $name = pop;
    $name = $1 if $name =~ /^([a-zA-Z0-9_]+)/;
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

package FusqlFS::Backend::PgSQL::Role::Owned;
use parent 'FusqlFS::Artifact';

1;

package FusqlFS::Backend::PgSQL::Roles;
use parent 'FusqlFS::Artifact';
use DBI qw(:sql_types);

=begin testing SETUP

#!class FusqlFS::Backend::PgSQL::Test

my $new_role = {
    struct => q{---
can_login: 1
cat_update: 1
config: ~
conn_limit: 1
create_db: 1
create_role: 1
inherit: 0
superuser: 1
valid_until: '2010-01-01 00:00:00+02'
},
    postgres => \"../postgres",
};

=end testing
=cut

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

    $self->{create_expr} = 'CREATE ROLE "%s"';
    $self->{rename_expr} = 'ALTER ROLE "%s" RENAME TO "%s"';
    $self->{drop_expr} = 'DROP ROLE "%s"';

    $self->{revoke_expr} = 'REVOKE "%s" FROM "%s"';
    $self->{grant_expr} = 'GRANT "%s" TO "%s"';

    bless $self, $class;
}

=begin testing get

is $_tobj->get('unknown'), undef, 'Unknown role not exists';
is_deeply $_tobj->get('postgres'), { struct => q{---
can_login: 1
cat_update: 1
config: ~
conn_limit: '-1'
create_db: 1
create_role: 1
inherit: 1
superuser: 1
valid_until: ~
} }, 'Known role is sane';

=end testing
=cut
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

=begin testing list

list_ok $_tobj->list(), sub { grep { $_ eq 'postgres' } @_ }, 'Roles list is sane';

=end testing
=cut
sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr})||[];
}

=begin testing rename after store

isnt $_tobj->rename('fusqlfs_test', 'new_fusqlfs_test'), undef, 'Role renamed';
is_deeply $_tobj->get('new_fusqlfs_test'), $new_role, 'Role renamed correctly';
is $_tobj->get('fusqlfs_test'), undef, 'Role is unaccessable under old name';
my $list = $_tobj->list();
ok grep { $_ eq 'new_fusqlfs_test' } @$list;
ok !grep { $_ eq 'fusqlfs_test' } @$list;

=end testing
=cut
sub rename
{
    my $self = shift;
    my ($name, $newname) = @_;
    $self->do($self->{rename_expr}, [$name, $newname]);
}

=begin testing drop after rename

isnt $_tobj->drop('new_fusqlfs_test'), undef, 'Role deleted';
is $_tobj->get('new_fusqlfs_test'), undef, 'Deleted role is absent';
my $list = $_tobj->list();
ok !grep { $_ eq 'new_fusqlfs_test' } @$list;

=end testing
=cut
sub drop
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{drop_expr}, [$name]);
}

=begin testing create after get list

isnt $_tobj->create('fusqlfs_test'), undef, 'Role created';
is $_tobj->get('fusqlfs_test')->{struct}, q{---
can_login: 0
cat_update: 0
config: ~
conn_limit: '-1'
create_db: 0
create_role: 0
inherit: 1
superuser: 0
valid_until: ~
}, 'New role is sane';

my $list = $_tobj->list();
ok grep { $_ eq 'fusqlfs_test' } @$list;

=end testing
=cut
sub create
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{create_expr}, [$name]);
}

=begin testing store after create

isnt $_tobj->store('fusqlfs_test', $new_role), undef, 'Role saved';
is_deeply $_tobj->get('fusqlfs_test'), $new_role, 'Role saved correctly';

=end testing
=cut
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

    my $sth = $self->build("ALTER ROLE \"$name\" ", sub{
            my ($a, $b) = @$_;
            if (ref $b)
            {
                return unless $data->{$a};
                return "$b->[0] ? ", $data->{$a}, $b->[1];
            }
            else
            {
                return unless exists $data->{$a};
                return ($data->{$a}? '': 'NO') . "$b ";
            }
    }, [ superuser   => 'SUPERUSER'  ],
       [ create_db   => 'CREATEDB'   ],
       [ create_role => 'CREATEROLE' ],
       [ inherit     => 'INHERIT'    ],
       [ can_login   => 'LOGIN'      ],
       [ conn_limit  => ['CONNECTION LIMIT', SQL_INTEGER] ],
       [ valid_until => ['VALID UNTIL', SQL_TIMESTAMP]    ],
       [ password    => ['PASSWORD', SQL_VARCHAR]         ]);

    $sth->execute();
}

1;

