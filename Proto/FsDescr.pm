package Interface;

sub new { bless {}, $_[0] }
sub get { return '' }
sub list { return [] }
sub rename { return 1 }
sub drop { return 1 }
sub create { return 1 }
sub store { return 1 }

1;

package FsDescr;

use DBI;
use YAML::Tiny;
use Data::Dump qw(dump);

use strict;
use feature ':5.10';

$, = ", ";

our $dumper = \&YAML::Tiny::Dump;
our $dbh;

sub new
{
    my $class = shift;
    $dbh = DBI->connect(@_);
    my $self = {
        subpackages => {
            tables => new Tables(),
        },
    };

    bless $self, $class;
}

1;


package Roles;
use base 'Interface';

sub get
{
    my $self = shift;
    my ($name) = @_;
    return {
        owner => \"../../owner",
        owned => new Role::Owned($self->{dbh}),
        permissions => new Role::Permissioms($self->{dbh}),
        password => sub() {},
        'create.sql' => '',
    };
}

1;

package Role::Permissioms;
use base 'Interface';

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

package Role::Owned;
use base 'Interface';


1;

package Views;
use base 'Interface';

1;

package Functions;
use base 'Interface';

1;

package Queries;
use base 'Interface';

1;

package Table::Data;
use base 'Interface';

sub list
{
    my $self = shift;
    my ($table) = @_;
    my $primary_key = join ' || ', Table::Indices->new($self->{dbh})->get_primary_key($table);
    my $sth = $FsDescr::dbh->prepare_cached(sprintf('SELECT %s FROM "%s"', $primary_key, $table));
    return $FsDescr::dbh->selectcol_arrayref($sth);
}

sub where_clause
{
    my $self = shift;
    my ($table) = @_;
    return join ' AND ', map { "\"$_\" = ?" } Table::Indices->new($self->{dbh})->get_primary_key($table);
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $sth = $FsDescr::dbh->prepare_cached(sprintf('SELECT * FROM "%s" WHERE %s', $table, $self->where_clause($table)));
    return &$FsDescr::dumper($sth->fetchrow_hashref) if $sth->execute(split /[.]/, $name);
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    my $sth = $FsDescr::dbh->prepare_cached(sprintf('DELETE FROM "%s" WHERE %s', $table, $self->where_clause($table)));
    $sth->execute(split /[.]/, $name);
}

sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;
    my $template = join ', ', map { "\"$_\" = ?" } keys %$data;
    my $sth = $FsDescr::dbh->prepare_cached(sprintf('UPDATE "%s" SET %s WHERE %s', $table, $template, $self->where_clause($table)));
    $sth->execute(values %$data, split /[.]/, $name);
}

sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    my @primary_key = Table::Indices->new($self->{dbh})->get_primary_key($table);
    my $pholders = '?,' x scalar(@primary_key);
    chop $pholders;
    my $sth = $FsDescr::dbh->prepare_cached(sprintf('INSERT INTO "%s" (%s) VALUES (%s)', $table, join(', ', @primary_key), $pholders));
    $sth->execute(split /[.]/, $name);
}

sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    my @primary_key = Table::Indices->new($self->{dbh})->get_primary_key($table);
    my %data = map { shift(@primary_key) => $_ } split /[.]/, $newname;
    $self->store($table, $name, \%data);
}

1;

package Table::Struct;
use base 'Interface';

sub new
{
    my $class = shift;
    my $self = {};
    $self->{list_expr} = $FsDescr::dbh->prepare("SELECT attname FROM pg_catalog.pg_attribute as a
                WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r') AND attnum > 0
            ORDER BY attnum");
    $self->{get_expr} = $FsDescr::dbh->prepare("SELECT typname as Type, pg_catalog.format_type(atttypid, atttypmod) AS Type_name,
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
    return $FsDescr::dbh->selectcol_arrayref($self->{list_expr}, {}, $table);
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $result = $FsDescr::dbh->selectrow_hashref($self->{get_expr}, {}, $table, $name);
    return &$FsDescr::dumper($result);
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    $FsDescr::dbh->do(sprintf($self->{drop_expr}, $table, $name));
}

sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    $FsDescr::dbh->do(sprintf($self->{create_expr}, $table, $name));
}

sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    $FsDescr::dbh->do(sprintf($self->{rename_expr}, $table, $name, $newname));
}

sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;

    my $newtype = $data->{'type_name'};
    my $length = $data->{'length'};
    $length .= ",$data->{decimal}" if $data->{'decimal'};
    $newtype .= "($length)" if $length;
    $newtype .= '[]' x $data->{'dimensions'};

    if (defined $data->{'default'}) {
        $FsDescr::dbh->do(sprintf($self->{store_default_expr}, $table, $name), {}, $data->{'default'});
    } else {
        $FsDescr::dbh->do(sprintf($self->{drop_default_expr}, $table, $name));
    }
    $FsDescr::dbh->do(sprintf($self->{$data->{'nullable'}? 'set_nullable_expr': 'drop_nullable_expr'}, $table, $name));
    $FsDescr::dbh->do(sprintf($self->{store_type_expr}, $table, $name, $newtype));
}

1;

package Table::Indices;
use base 'Interface';

sub new
{
    my $class = shift;
    my $self = {};
    $self->{rename_expr} = 'ALTER INDEX "%s" RENAME TO "%s"';
    $self->{drop_expr} = 'DROP INDEX "%s"';
    $self->{create_expr} = 'CREATE INDEX "%s" ON "%s" (%s)';

    $self->{list_expr} = $FsDescr::dbh->prepare("SELECT (SELECT c1.relname FROM pg_catalog.pg_class as c1 WHERE c1.oid = indexrelid) as Index_name
        FROM pg_catalog.pg_index
            WHERE indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')");
    $self->{get_expr} = $FsDescr::dbh->prepare("SELECT pg_get_indexdef(indexrelid, 0, true) AS \"create.sql\",
            indisunique as \".unique\", indisprimary as \".primary\", indkey as \".order\"
        FROM pg_catalog.pg_index
            WHERE indexrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'i')");

    $self->{get_primary_expr} = $FsDescr::dbh->prepare("SELECT indkey FROM pg_catalog.pg_index
            WHERE indisprimary AND indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')");
    $self->{create_cache} = {};

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $result = $FsDescr::dbh->selectrow_hashref($self->{get_expr}, {}, $name);
    if ($result->{'.order'})
    {
        my @fields = @{Table::Struct->new($self->{dbh})->list($table)};
        $result->{'.order'} = [ map { $fields[$_-1] } split / /, $result->{'.order'} ];
        $result->{$_} = \"../../$_" foreach @{$result->{'.order'}};
    }
    delete $result->{'.unique'} unless $result->{'.unique'};
    delete $result->{'.primary'} unless $result->{'.primary'};
    return $result;
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    return $FsDescr::dbh->selectcol_arrayref($self->{list_expr}, {}, $table) || [];
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    $FsDescr::dbh->do(sprintf($self->{drop_expr}, $name));
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
    $FsDescr::dbh->do(sprintf($self->{create_expr}, $name, $table, $data));
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
    $FsDescr::dbh->do(sprintf($self->{rename_expr}, $name, $newname));
}

sub get_primary_key
{
    my $self = shift;
    my ($table) = @_;
    my @result = ();
    my $data = $FsDescr::dbh->selectcol_arrayref($self->{get_primary_expr}, {}, $table);
    if ($data)
    {
        my $fields = Table::Struct->new($self->{dbh})->list($table);
        @result = map { $fields->[$_-1] } split / /, $data->[0];
    }
    return @result;
}

1;

package Tables;
use base 'Interface';

sub new
{
    my $class = shift;
    my $self = {};
    $self->{rename_expr} = 'ALTER TABLE "%s" RENAME TO "%s"';
    $self->{drop_expr} = 'DROP TABLE "%s"';
    $self->{create_expr} = 'CREATE TABLE "%s" (id serial, PRIMARY KEY (id))';

    $self->{list_expr} = $FsDescr::dbh->prepare("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'");

    $self->{subpackages} = {
        indices => new Table::Indices(),
        struct  => new Table::Struct(),
        data    => new Table::Data(),
    };

    bless $self, $class;
}

sub drop
{
    my $self = shift;
    my ($name) = @_;
    $FsDescr::dbh->do(sprintf($self->{drop_expr}, $name));
}

sub create
{
    my $self = shift;
    my ($name) = @_;
    $FsDescr::dbh->do(sprintf($self->{create_expr}, $name));
}

sub rename
{
    my $self = shift;
    my ($name, $newname) = @_;
    $FsDescr::dbh->do(sprintf($self->{rename_expr}, $name, $newname));
}

sub list
{
    my $self = shift;
    return $FsDescr::dbh->selectcol_arrayref($self->{list_expr}) || [];
}

1;
