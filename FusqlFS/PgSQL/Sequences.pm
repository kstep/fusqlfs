use strict;
use v5.10.0;
use FusqlFS::Base;

package FusqlFS::PgSQL::Sequences;
use base 'FusqlFS::Base::Interface';
use DBI qw(:sql_types);

sub new
{
    my $class = shift;
    my $self = {};

    $self->{list_expr} = $class->expr("SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'S'");
    $self->{exists_expr} = $class->expr("SELECT 1 FROM pg_catalog.pg_class WHERE relkind = 'S' AND relname = ?");
    $self->{get_expr} = 'SELECT * FROM "%s"';
    $self->{rename_expr} = 'ALTER SEQUENCE "%s" RENAME TO "%s"';

    $self->{subpackages} = {
    };

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($name) = @_;
    my $result = $self->all_col($self->{exists_expr}, $name);
    return unless @$result;
    return { struct => $self->dump($self->one_row($self->{get_expr}, [$name])) };
}

sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr}) || [];
}

sub store
{
    my $self = shift;
    my ($name, $data) = @_;
    $data = $self->load($data->{struct})||{};

    my $sql = "ALTER SEQUENCE \"$name\" ";

    my %params = (
        increment_by => ['INCREMENT BY', SQL_INTEGER, 0],
        cache_value  => ['CACHE', SQL_INTEGER, 0],
        last_value   => ['RESTART WITH', SQL_INTEGER, 0],
        max_value    => ['MAXVALUE', SQL_INTEGER, 1],
        min_value    => ['MINVALUE', SQL_INTEGER, 1],
    );

    my @binds = ();
    my @types = ();
    foreach (keys %params)
    {
        next unless exists $data->{$_};
        if (!defined $data->{$_})
        {
            $sql .= 'NO '.$params{$_}->[0].' ' if $params{$_}->[2];
            next;
        }
        $sql .= $params{$_}->[0].' ? ';
        push @binds, $data->{$_};
        push @types, $params{$_}->[1];
    }

    if (exists $data->{is_cycled})
    {
        $sql .= $data->{is_cycled}? 'CYCLE ': 'NO CYCLE ';
    }

    my $sth = $self->expr($sql);
    foreach (0..$#binds)
    {
        $sth->bind_param($_+1, $binds[$_], $types[$_]);
    }
    $sth->execute();
}

sub rename
{
    my $self = shift;
    my ($name, $newname) = @_;
    $self->do($self->{rename_expr}, [$name, $newname]);
}

1;

