use v5.10.0;
use strict;
use FusqlFS::Base;

package PgSQL::Root;
use base 'Base::Root';

sub init
{
    $_[0]->{subpackages} = {
        tables => new PgSQL::Tables(),
    };
}

sub dsn
{
    my $self = shift;
    return 'Pg:'.$self->SUPER::dsn(@_);
}

1;

package PgSQL::Roles;
use base 'Base::Interface';

sub get
{
    my $self = shift;
    my ($name) = @_;
    return {
        owner => \"../../owner",
        owned => new PgSQL::Role::Owned($self->{dbh}),
        permissions => new PgSQL::Role::Permissions($self->{dbh}),
        password => sub() {},
        'create.sql' => '',
    };
}

1;

package PgSQL::Role::Permissions;
use base 'Base::Interface';

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

package PgSQL::Role::Owned;
use base 'Base::Interface';

1;

package PgSQL::Views;
use base 'Base::Interface';

1;

package PgSQL::Functions;
use base 'Base::Interface';

1;

package PgSQL::Queries;
use base 'Base::Interface';

1;

package PgSQL::Table::Data;
use base 'Base::Interface';

sub new
{
    my $class = shift;
    my $self = {};

    $self->{get_primary_expr} = $Base::Root::dbh->prepare("SELECT indkey FROM pg_catalog.pg_index
            WHERE indisprimary AND indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')");

    bless $self, $class;
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    my $primary_key = join ' || ', $self->get_primary_key($table);
    my $sth = $Base::Root::dbh->prepare_cached(sprintf('SELECT %s FROM "%s"', $primary_key, $table));
    return $Base::Root::dbh->selectcol_arrayref($sth);
}

sub where_clause
{
    my $self = shift;
    my ($table, $name) = @_;
    my @binds = split /[.]/, $name;
    my @primary_key = $self->get_primary_key($table);
    return unless $#primary_key == $#binds;
    return join(' AND ', map { "\"$_\" = ?" } @primary_key), @binds;
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my ($where_clause, @binds) = $self->where_clause($table, $name);
    return unless $where_clause;

    my $sth = $Base::Root::dbh->prepare_cached(sprintf('SELECT * FROM "%s" WHERE %s', $table, $where_clause));
    return &$Base::Root::dumper($sth->fetchrow_hashref) if $sth->execute(@binds);
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    my ($where_clause, @binds) = $self->where_clause($table, $name);
    return unless $where_clause;

    my $sth = $Base::Root::dbh->prepare_cached(sprintf('DELETE FROM "%s" WHERE %s', $table, $where_clause));
    $sth->execute(@binds);
}

sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;
    my ($where_clause, @binds) = $self->where_clause($table, $name);
    return unless $where_clause;

    $data = &$Base::Root::loader($data);
    my $template = join ', ', map { "\"$_\" = ?" } keys %$data;
    my $sth = $Base::Root::dbh->prepare_cached(sprintf('UPDATE "%s" SET %s WHERE %s', $table, $template, $where_clause));
    $sth->execute(values %$data, @binds);
}

sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    my @primary_key = $self->get_primary_key($table);
    my $pholders = '?,' x scalar(@primary_key);
    chop $pholders;
    my $sth = $Base::Root::dbh->prepare_cached(sprintf('INSERT INTO "%s" (%s) VALUES (%s)', $table, join(', ', @primary_key), $pholders));
    $sth->execute(split /[.]/, $name);
}

sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    my @primary_key = $self->get_primary_key($table);
    my %data = map { shift(@primary_key) => $_ } split /[.]/, $newname;
    $self->store($table, $name, \%data);
}

sub get_primary_key
{
    my $self = shift;
    my ($table) = @_;
    my @result = ();
    my $data = $Base::Root::dbh->selectcol_arrayref($self->{get_primary_expr}, {}, $table);
    if ($data)
    {
        my $fields = PgSQL::Table::Struct->new()->list($table);
        @result = map { $fields->[$_-1] } split / /, $data->[0];
    }
    return @result;
}


1;

package PgSQL::Table::Struct;
use base 'Base::Interface';

sub new
{
    my $class = shift;
    my $self = {};
    $self->{list_expr} = $Base::Root::dbh->prepare("SELECT attname FROM pg_catalog.pg_attribute as a
                WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r') AND attnum > 0
            ORDER BY attnum");
    $self->{get_expr} = $Base::Root::dbh->prepare("SELECT typname as Type, pg_catalog.format_type(atttypid, atttypmod) AS Type_name,
                NOT attnotnull as Nullable,
                CASE WHEN atthasdef THEN
                    (SELECT pg_catalog.pg_get_expr(adbin, adrelid) FROM pg_attrdef as d
                        WHERE adrelid = attrelid AND adnum = attnum)
                ELSE NULL END AS Default,
                CASE WHEN atttypmod < 0 THEN NULL
                    WHEN typcategory = 'N' THEN (((atttypmod-4)>>16)&65535)
                    ELSE atttypmod-4 END AS Length,
                CASE WHEN atttypmod < 0 THEN NULL
                    WHEN typcategory = 'N' THEN ((atttypmod-4)&65535)
                    ELSE NULL END AS Decimal,
                attndims AS Dimensions,
                attnum as Order
            FROM pg_catalog.pg_attribute as a, pg_catalog.pg_type as t
            WHERE a.atttypid = t.oid
                AND attrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')
                AND attname = ?");

    $self->{drop_expr} = 'ALTER TABLE "%s" DROP COLUMN "%s"';
    $self->{create_expr} = 'ALTER TABLE "%s" ADD COLUMN "%s" INTEGER NOT NULL DEFAULT \'0\'';
    $self->{rename_expr} = 'ALTER TABLE "%s" RENAME COLUMN "%s" TO "%s"';

    $self->{store_default_expr} = 'ALTER TABLE "%s" ALTER COLUMN "%s" SET DEFAULT ?';
    $self->{drop_default_expr} = 'ALTER TABLE "%s" ALTER COLUMN "%s" DROP DEFAULT';
    $self->{set_nullable_expr} = 'ALTER TABLE "%s" ALTER COLUMN "%s" DROP NOT NULL';
    $self->{drop_nullable_expr} = 'ALTER TABLE "%s" ALTER COLUMN "%s" SET NOT NULL';
    $self->{store_type_expr} = 'ALTER TABLE "%s" ALTER COLUMN "%s" TYPE %s';
    bless $self, $class;
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    return $Base::Root::dbh->selectcol_arrayref($self->{list_expr}, {}, $table);
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $result = $Base::Root::dbh->selectrow_hashref($self->{get_expr}, {}, $table, $name);
    return &$Base::Root::dumper($result);
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    $Base::Root::dbh->do(sprintf($self->{drop_expr}, $table, $name));
}

sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    $Base::Root::dbh->do(sprintf($self->{create_expr}, $table, $name));
}

sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    $Base::Root::dbh->do(sprintf($self->{rename_expr}, $table, $name, $newname));
}

sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;
    $data = &$Base::Root::loader($data);

    my $newtype = $data->{'type_name'};
    my $length = $data->{'length'};
    $length .= ",$data->{decimal}" if $data->{'decimal'};
    $newtype .= "($length)" if $length;
    $newtype .= '[]' x $data->{'dimensions'};

    if (defined $data->{'default'}) {
        $Base::Root::dbh->do(sprintf($self->{store_default_expr}, $table, $name), {}, $data->{'default'});
    } else {
        $Base::Root::dbh->do(sprintf($self->{drop_default_expr}, $table, $name));
    }
    $Base::Root::dbh->do(sprintf($self->{$data->{'nullable'}? 'set_nullable_expr': 'drop_nullable_expr'}, $table, $name));
    $Base::Root::dbh->do(sprintf($self->{store_type_expr}, $table, $name, $newtype));
}

1;

package PgSQL::Table::Indices;
use base 'Base::Interface';

sub new
{
    my $class = shift;
    my $self = {};
    $self->{rename_expr} = 'ALTER INDEX "%s" RENAME TO "%s"';
    $self->{drop_expr} = 'DROP INDEX "%s"';
    $self->{create_expr} = 'CREATE INDEX "%s" ON "%s" (%s)';

    $self->{list_expr} = $Base::Root::dbh->prepare("SELECT (SELECT c1.relname FROM pg_catalog.pg_class as c1 WHERE c1.oid = indexrelid) as Index_name
        FROM pg_catalog.pg_index
            WHERE indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')");
    $self->{get_expr} = $Base::Root::dbh->prepare("SELECT pg_get_indexdef(indexrelid, 0, true) AS \"create.sql\",
            indisunique as \".unique\", indisprimary as \".primary\", indkey as \".order\"
        FROM pg_catalog.pg_index
            WHERE indexrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'i')");

    $self->{create_cache} = {};

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $result = $Base::Root::dbh->selectrow_hashref($self->{get_expr}, {}, $name);
    return unless $result;
    if ($result->{'.order'})
    {
        my @fields = @{PgSQL::Table::Struct->new()->list($table)};
        $result->{'.order'} = [ map { $fields[$_-1] } split / /, $result->{'.order'} ];
        $result->{$_} = \"../../struct/$_" foreach @{$result->{'.order'}};
    }
    delete $result->{'.unique'} unless $result->{'.unique'};
    delete $result->{'.primary'} unless $result->{'.primary'};
    return $result;
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    return $Base::Root::dbh->selectcol_arrayref($self->{list_expr}, {}, $table) || [];
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    $Base::Root::dbh->do(sprintf($self->{drop_expr}, $name));
}

sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;
    if (exists $self->{create_cache}->{$table}->{$name})
    {
        delete $self->{create_cache}->{$table}->{$name};
    }
    else
    {
        $self->drop($table, $name);
    }
    $Base::Root::dbh->do(sprintf($self->{create_expr}, $name, $table, $data));
}

sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->{create_cache}->{$table} ||= {};
    $self->{create_cache}->{$table}->{$name} = 1;
}

sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    $Base::Root::dbh->do(sprintf($self->{rename_expr}, $name, $newname));
}

1;

package PgSQL::Tables;
use base 'Base::Interface';

sub new
{
    my $class = shift;
    my $self = {};
    $self->{rename_expr} = 'ALTER TABLE "%s" RENAME TO "%s"';
    $self->{drop_expr} = 'DROP TABLE "%s"';
    $self->{create_expr} = 'CREATE TABLE "%s" (id serial, PRIMARY KEY (id))';

    $self->{list_expr} = $Base::Root::dbh->prepare("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'");
    $self->{get_expr} = $Base::Root::dbh->prepare("SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename = ?");

    $self->{subpackages} = {
        indices => new PgSQL::Table::Indices(),
        struct  => new PgSQL::Table::Struct(),
        data    => new PgSQL::Table::Data(),
    };

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($name) = @_;
    my $result = $Base::Root::dbh->selectcol_arrayref($self->{get_expr}, {}, $name);
    return $self->{subpackages} if @$result;
}

sub drop
{
    my $self = shift;
    my ($name) = @_;
    $Base::Root::dbh->do(sprintf($self->{drop_expr}, $name));
}

sub create
{
    my $self = shift;
    my ($name) = @_;
    $Base::Root::dbh->do(sprintf($self->{create_expr}, $name));
}

sub rename
{
    my $self = shift;
    my ($name, $newname) = @_;
    $Base::Root::dbh->do(sprintf($self->{rename_expr}, $name, $newname));
}

sub list
{
    my $self = shift;
    return $Base::Root::dbh->selectcol_arrayref($self->{list_expr}) || [];
}

1;
