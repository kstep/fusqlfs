use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Sequences;
use parent 'FusqlFS::Interface';
use FusqlFS::Backend::PgSQL::Roles;
use DBI qw(:sql_types);

sub new
{
    my $class = shift;
    my $self = {};

    $self->{list_expr} = $class->expr("SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'S'");
    $self->{exists_expr} = $class->expr("SELECT 1 FROM pg_catalog.pg_class WHERE relkind = 'S' AND relname = ?");
    $self->{get_expr} = 'SELECT * FROM "%s"';
    $self->{rename_expr} = 'ALTER SEQUENCE "%s" RENAME TO "%s"';
    $self->{create_expr} = 'CREATE SEQUENCE "%s"';
    $self->{drop_expr} = 'DROP SEQUENCE "%s"';

    $self->{owner} = new FusqlFS::Backend::PgSQL::Role::Owner('S', 2);

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($name) = @_;
    my $result = $self->all_col($self->{exists_expr}, $name);
    return unless @$result;
    return {
        struct => $self->dump($self->one_row($self->{get_expr}, [$name])),
        owner  => $self->{owner},
    };
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
    $sql .= $data->{is_cycled}? 'CYCLE ': 'NO CYCLE ' if exists $data->{is_cycled};

    my $sth = $self->build($sql, sub{
            my ($a, $b) = @$_;
            return unless exists $data->{$a};
            if (!defined $data->{$a})
            {
                return "NO $b->[0] " if $b->[2];
                return;
            }
            return "$b->[0] ? ", $data->{$a}, $b->[1];
    }, [ increment_by => ['INCREMENT BY', SQL_INTEGER, 0] ],
       [ cache_value  => ['CACHE', SQL_INTEGER, 0]        ],
       [ last_value   => ['RESTART WITH', SQL_INTEGER, 0] ],
       [ max_value    => ['MAXVALUE', SQL_INTEGER, 1]     ],
       [ min_value    => ['MINVALUE', SQL_INTEGER, 1]     ]);

    $sth->execute();
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

sub create
{
    my $self = shift;
    my ($name) = @_;
    $self->do($self->{create_expr}, [$name]);
}

1;

