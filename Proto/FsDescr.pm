package FsDescr;

use DBI;
use YAML::Tiny;
use Data::Dump qw(dump);

use strict;
use feature ':5.10';

$, = ", ";

sub new
{
    my $class = shift;
    my $dbh = DBI->connect(@_);
    my $self = {
        dbh => $dbh,
        subpackages => {
            tables => new Tables($dbh),
        },
    };

    bless $self, $class;
}

1;

package Table::Data;

sub new
{
    my $class = shift;
    my $dbh = shift;
    my $self = {
        dbh => $dbh,
    };
    bless $self, $class;
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    my $primary_key = join ' || ', Table::Indices->new($self->{dbh})->get_primary_key($table);
    my $sth = $self->{dbh}->prepare_cached(sprintf('SELECT %s FROM "%s"', $primary_key, $table));
    return $self->{dbh}->selectcol_arrayref($sth);
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
    my $sth = $self->{dbh}->prepare_cached(sprintf('SELECT * FROM "%s" WHERE %s', $table, $self->where_clause($table)));
    return YAML::Tiny::Dump($sth->fetchrow_hashref) if $sth->execute(split /[.]/, $name);
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    my $sth = $self->{dbh}->prepare_cached(sprintf('DELETE FROM "%s" WHERE %s', $table, $self->where_clause($table)));
    $sth->execute(split /[.]/, $name);
}

sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;
    my $template = join ', ', map { "\"$_\" = ?" } keys %$data;
    my $sth = $self->{dbh}->prepare_cached(sprintf('UPDATE "%s" SET %s WHERE %s', $table, $template, $self->where_clause($table)));
    $sth->execute(values %$data, split /[.]/, $name);
}

sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    my @primary_key = Table::Indices->new($self->{dbh})->get_primary_key($table);
    my $pholders = '?,' x scalar(@primary_key);
    chop $pholders;
    my $sth = $self->{dbh}->prepare_cached(sprintf('INSERT INTO "%s" (%s) VALUES (%s)', $table, join(', ', @primary_key), $pholders));
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

sub new
{
    my $class = shift;
    my $dbh = shift;
    my $self = {
        dbh => $dbh,
        list_expr => $dbh->prepare("SELECT attname FROM pg_catalog.pg_attribute as a
                WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r') AND attnum > 0
            ORDER BY attnum"),
        get_expr => $dbh->prepare("SELECT typname as Type, pg_catalog.format_type(atttypid, atttypmod) AS Type_name,
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
                AND attname = ?"),

        drop_expr => 'ALTER TABLE "%s" DROP COLUMN "%s"',
        create_expr => 'ALTER TABLE "%s" ADD COLUMN "%s" INTEGER NOT NULL DEFAULT \'0\'',
        rename_expr => 'ALTER TABLE "%s" RENAME COLUMN "%s" TO "%s"',

        store_default_expr => 'ALTER TABLE "%s" ALTER COLUMN "%s" SET DEFAULT ?',
        drop_default_expr  => 'ALTER TABLE "%s" ALTER COLUMN "%s" DROP DEFAULT',
        set_nullable_expr  => 'ALTER TABLE "%s" ALTER COLUMN "%s" DROP NOT NULL',
        drop_nullable_expr => 'ALTER TABLE "%s" ALTER COLUMN "%s" SET NOT NULL',
        store_type_expr    => 'ALTER TABLE "%s" ALTER COLUMN "%s" TYPE %s',
    };
    bless $self, $class;
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    return $self->{dbh}->selectcol_arrayref($self->{list_expr}, {}, $table);
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $result = $self->{dbh}->selectrow_hashref($self->{get_expr}, {}, $table, $name);
    return YAML::Tiny::Dump($result);
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->{dbh}->do(sprintf($self->{drop_expr}, $table, $name));
}

sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->{dbh}->do(sprintf($self->{create_expr}, $table, $name));
}

sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    $self->{dbh}->do(sprintf($self->{rename_expr}, $table, $name, $newname));
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
        $self->{dbh}->do(sprintf($self->{store_default_expr}, $table, $name), {}, $data->{'default'});
    } else {
        $self->{dbh}->do(sprintf($self->{drop_default_expr}, $table, $name));
    }
    $self->{dbh}->do(sprintf($self->{$data->{'nullable'}? 'set_nullable_expr': 'drop_nullable_expr'}, $table, $name));
    $self->{dbh}->do(sprintf($self->{store_type_expr}, $table, $name, $newtype));
}

1;

package Table::Indices;

sub new
{
    my $class = shift;
    my $dbh = shift;
    my $self = {
        rename_expr => 'ALTER INDEX "%s" RENAME TO "%s"',
        drop_expr   => 'DROP INDEX "%s"',
        create_expr => 'CREATE INDEX "%s" ON "%s" (%s)',

        list_expr   => $dbh->prepare("SELECT (SELECT c1.relname FROM pg_catalog.pg_class as c1 WHERE c1.oid = indexrelid) as Index_name
            FROM pg_catalog.pg_index
                WHERE indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')"),
        get_expr    => $dbh->prepare("SELECT pg_get_indexdef(indexrelid, 0, true) AS \"create.sql\",
                indisunique as \".unique\", indisprimary as \".primary\", indkey as \".order\"
            FROM pg_catalog.pg_index
                WHERE indexrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'i')"),

        get_primary_expr => $dbh->prepare("SELECT indkey FROM pg_catalog.pg_index
                WHERE indisprimary AND indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')"),
        create_cache => {},
        dbh => $dbh,
    };
    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $result = $self->{dbh}->selectrow_hashref($self->{get_expr}, {}, $name);
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
    return $self->{dbh}->selectcol_arrayref($self->{list_expr}, {}, $table) || [];
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->{dbh}->do(sprintf($self->{drop_expr}, $name));
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
    $self->{dbh}->do(sprintf($self->{create_expr}, $name, $table, $data));
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
    $self->{dbh}->do(sprintf($self->{rename_expr}, $name, $newname));
}

sub get_primary_key
{
    my $self = shift;
    my ($table) = @_;
    my @result = ();
    my $data = $self->{dbh}->selectcol_arrayref($self->{get_primary_expr}, {}, $table);
    if ($data)
    {
        my $fields = Table::Struct->new($self->{dbh})->list($table);
        @result = map { $fields->[$_-1] } split / /, $data->[0];
    }
    return @result;
}

1;

package Tables;

sub new
{
    my $class = shift;
    my $dbh = shift;
    my $self = {
	rename_expr => 'ALTER TABLE "%s" RENAME TO "%s"',
	drop_expr   => 'DROP TABLE "%s"',
	create_expr => 'CREATE TABLE "%s" (id serial, PRIMARY KEY (id))',

        list_expr   => $dbh->prepare("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'"),
	dbh => $dbh,

        subpackages => {
            indices => new Table::Indices($dbh),
            struct  => new Table::Struct($dbh),
            data    => new Table::Data($dbh),
        }
    };


    bless $self, $class;
}

sub drop
{
    my $self = shift;
    my ($name) = @_;
    $self->{dbh}->do(sprintf($self->{drop_expr}, $name));
}

sub create
{
    my $self = shift;
    my ($name) = @_;
    $self->{dbh}->do(sprintf($self->{create_expr}, $name));
}

sub rename
{
    my $self = shift;
    my ($name, $newname) = @_;
    $self->{dbh}->do(sprintf($self->{rename_expr}, $name, $newname));
}

sub list
{
    my $self = shift;
    return $self->{dbh}->selectcol_arrayref($self->{list_expr}) || [];
}

1;
