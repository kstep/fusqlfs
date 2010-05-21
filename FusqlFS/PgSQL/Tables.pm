use strict;
use v5.10.0;

use FusqlFS::Base;

package FusqlFS::PgSQL::Table::Data;
use base 'FusqlFS::Base::Interface';

sub new
{
    my $class = shift;
    my $self = {};

    $self->{get_primary_expr} = $class->expr("SELECT indkey FROM pg_catalog.pg_index
            WHERE indisprimary AND indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')");

    $self->{query_cache} = {};

    bless $self, $class;
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    my $primary_key = join " || '.' || ", $self->get_primary_key($table);
    my $sth = $self->cexpr('SELECT %s FROM "%s" %s', $primary_key, $table, $self->limit());
    return $self->all_col($sth);
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

    $self->{query_cache}->{$table} ||= {};
    $self->{query_cache}->{$table}->{$where_clause} ||= $self->expr('SELECT * FROM "%s" WHERE %s LIMIT 1', $table, $where_clause);

    my $sth = $self->{query_cache}->{$table}->{$where_clause};
    return $self->dump($sth->fetchrow_hashref) if $sth->execute(@binds);
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    my ($where_clause, @binds) = $self->where_clause($table, $name);
    return unless $where_clause;

    $self->cdo('DELETE FROM "%s" WHERE %s', [$table, $where_clause], @binds);
}

sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;
    my ($where_clause, @binds) = $self->where_clause($table, $name);
    return unless $where_clause;

    $data = $self->load($data);
    my $template = join ', ', map { "\"$_\" = ?" } keys %$data;
    $self->cdo('UPDATE "%s" SET %s WHERE %s', [$table, $template, $where_clause], values %$data, @binds);
}

sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    my @primary_key = $self->get_primary_key($table);
    my $pholders = '?,' x scalar(@primary_key);
    chop $pholders;
    $self->cdo('INSERT INTO "%s" (%s) VALUES (%s)', [$table, join(', ', @primary_key), $pholders], split(/[.]/, $name));
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
    my $data = $self->all_col($self->{get_primary_expr}, $table);
    if ($data)
    {
        my $fields = FusqlFS::PgSQL::Table::Struct->new()->list($table);
        @result = map { $fields->[$_-1] } split / /, $data->[0];
    }
    return @result;
}

1;

package FusqlFS::PgSQL::Table::Struct;
use base 'FusqlFS::Base::Interface';

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

package FusqlFS::PgSQL::Table::Indices;
use base 'FusqlFS::Base::Interface';

sub new
{
    my $class = shift;
    my $self = {};
    $self->{rename_expr} = 'ALTER INDEX "%s" RENAME TO "%s"';
    $self->{drop_expr} = 'DROP INDEX "%s"';
    $self->{create_expr} = 'CREATE %s INDEX "%s" ON "%s" (%s)';

    $self->{list_expr} = $class->expr("SELECT (SELECT c1.relname FROM pg_catalog.pg_class as c1 WHERE c1.oid = indexrelid) as Index_name
        FROM pg_catalog.pg_index
            WHERE indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')");
    $self->{get_expr} = $class->expr("SELECT pg_get_indexdef(indexrelid, 0, true) AS \"create.sql\",
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
    return $self->{create_cache}->{$table}->{$name} if exists $self->{create_cache}->{$table}->{$name};

    my $result = $self->one_row($self->{get_expr}, $name);
    return unless $result;
    if ($result->{'.order'})
    {
        my @fields = @{FusqlFS::PgSQL::Table::Struct->new()->list($table)};
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
    my @list = keys %{$self->{create_cache}->{$table}||{}};
    return [ (@{$self->all_col($self->{list_expr}, $table)}, @list) ] || \@list;
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->do($self->{drop_expr}, [$name]);
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
    my $fields = $self->parse_fields($data);
    my $unique = defined $data->{'.unique'}? 'UNIQUE': '';
    $self->do($self->{create_expr}, [$unique, $name, $table, $fields]);
}

sub parse_fields
{
    my $self = shift;
    my ($data) = @_;
    my @order = grep { exists $data->{$_} } @{$data->{'.order'}};
    my @fields = grep { !/^\./ && $_ ne 'create.sql' } keys %$data;

    my %order = map { $_ => 1 } @order;
    foreach (@fields)
    {
        push @order, $_ unless exists $order{$_};
    }
    my $fields = '"'.join('", "', @order).'"';

    return $fields;
}

sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->{create_cache}->{$table} ||= {};
    $self->{create_cache}->{$table}->{$name} = { '.order' => [] };
}

sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    $self->do($self->{rename_expr}, [$name, $newname]);
}

1;

package FusqlFS::PgSQL::Table::Constraints;
use base 'FusqlFS::Base::Interface';

sub new
{
    my $class = shift;
    my $self = {};

    $self->{get_expr} = $class->expr('SELECT pg_catalog.pg_get_constraintdef(co.oid, true) AS struct, co.contype AS ".type" FROM pg_catalog.pg_constraint co
            JOIN pg_catalog.pg_class AS cl ON (cl.oid = co.conrelid) WHERE cl.relname = ? AND co.conname = ?');
    $self->{list_expr} = $class->expr('SELECT co.conname FROM pg_catalog.pg_constraint AS co
            JOIN pg_catalog.pg_class AS cl ON (cl.oid = co.conrelid) WHERE cl.relname = ?');

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $data = $self->one_row($self->{get_expr}, $table, $name);
    return unless $data;
    if ($data->{".type"} eq 'f')
    {
        my ($myfields, $table, $herfields) = ($data->{struct} =~ /KEY \((.+?)\) REFERENCES (.+?)\((.+?)\)/);
        my @myfields = split /,/, $myfields;
        my @herfields = split /,/, $herfields;
        foreach (0..$#myfields)
        {
            $data->{$myfields[$_]} = \"../../../$table/struct/$herfields[$_]";
        }
    }
    return $data;
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    return $self->all_col($self->{list_expr}, $table);
}

1;

package FusqlFS::PgSQL::Tables;
use base 'FusqlFS::Base::Interface';
use FusqlFS::PgSQL::Roles;

sub new
{
    my $class = shift;
    my $self = {};
    $self->{rename_expr} = 'ALTER TABLE "%s" RENAME TO "%s"';
    $self->{drop_expr} = 'DROP TABLE "%s"';
    $self->{create_expr} = 'CREATE TABLE "%s" (id serial, PRIMARY KEY (id))';

    $self->{list_expr} = $class->expr("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'");
    $self->{get_expr} = $class->expr("SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename = ?");

    $self->{subpackages} = {
        indices     => new FusqlFS::PgSQL::Table::Indices(),
        struct      => new FusqlFS::PgSQL::Table::Struct(),
        data        => new FusqlFS::PgSQL::Table::Data(),
        constraints => new FusqlFS::PgSQL::Table::Constraints(),
        owner       => new FusqlFS::PgSQL::Role::Owner('r', 2),
    };

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($name) = @_;
    my $result = $self->all_col($self->{get_expr}, $name);
    return unless @$result;
    return $self->{subpackages};
}

sub drop
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{drop_expr}, [$name]);
}

sub create
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{create_expr}, [$name]);
}

sub rename
{
    my $self = shift;
    my ($name, $newname) = @_;
    $self->do($self->{rename_expr}, [$name, $newname]);
}

sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr}) || [];
}

1;

