use strict;
use v5.10.0;

use FusqlFS::Interface;

package FusqlFS::Backend::PgSQL::Table::Struct;
use base 'FusqlFS::Interface';

sub new
{
    my $class = shift;
    my $self = {};
    $self->{list_expr} = $class->expr("SELECT attname FROM pg_catalog.pg_attribute as a
                WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r') AND attnum > 0
            ORDER BY attnum");
    $self->{get_expr} = $class->expr("SELECT pg_catalog.format_type(atttypid, atttypmod) AS type,
                NOT attnotnull as nullable,
                CASE WHEN atthasdef THEN
                    (SELECT pg_catalog.pg_get_expr(adbin, adrelid) FROM pg_attrdef as d
                        WHERE adrelid = attrelid AND adnum = attnum)
                ELSE NULL END AS default,
                attndims AS dimensions,
                attnum as order
            FROM pg_catalog.pg_attribute as a
            WHERE attrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')
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
    return $self->all_col($self->{list_expr}, $table);
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $result = $self->one_row($self->{get_expr}, $table, $name);
    return $self->dump($result);
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->do($self->{drop_expr}, [$table, $name]);
}

sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->do($self->{create_expr}, [$table, $name]);
}

sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    $self->do($self->{rename_expr}, [$table, $name, $newname]);
}

sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;
    $data = $self->load($data);

    my $newtype = $data->{'type'};
    $newtype =~ s/(\[\])+$//;
    $newtype .= '[]' x $data->{'dimensions'};

    my $using = $data->{'using'} || undef;
    $newtype .= " USING $using" if $using;

    if (defined $data->{'default'}) {
        $self->do($self->{store_default_expr}, [$table, $name], $data->{'default'});
    } else {
        $self->do($self->{drop_default_expr}, [$table, $name]);
    }
    $self->do($self->{$data->{'nullable'}? 'set_nullable_expr': 'drop_nullable_expr'}, [$table, $name]);
    $self->do($self->{store_type_expr}, [$table, $name, $newtype]);
}

1;

