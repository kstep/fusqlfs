use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Languages;
use parent 'FusqlFS::Artifact';

use FusqlFS::Backend::PgSQL::Roles;

sub new
{
    my $class = shift;
    my $self = {};

    $self->{get_expr} = $class->expr('SELECT l.lanispl AS ispl, l.lanpltrusted AS trusted,
            hp.proname||\'(\'||pg_catalog.pg_get_function_arguments(hp.oid)||\')\' AS handler,
            vp.proname||\'(\'||pg_catalog.pg_get_function_arguments(vp.oid)||\')\' AS validator
        FROM pg_catalog.pg_language AS l
            LEFT JOIN pg_catalog.pg_proc AS hp ON (l.lanplcallfoid = hp.oid)
            LEFT JOIN pg_catalog.pg_proc AS vp ON (l.lanvalidator = vp.oid)
        WHERE lanname = ?');
    $self->{list_expr} = $class->expr('SELECT lanname FROM pg_catalog.pg_language');

    $self->{owner} = FusqlFS::Backend::PgSQL::Role::Owner->new('_L', 2);

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($name) = @_;
    my $data = $self->one_row($self->{get_expr}, $name);
    return unless $data;

    my $result = {};
    $result->{handler}   = \"../../functions/$data->{handler}"   if $data->{handler};
    $result->{validator} = \"../../functions/$data->{validator}" if $data->{validator};
    delete $data->{handler};
    delete $data->{validator};

    $result->{struct} = $self->dump($data);
    $result->{owner}  = $self->{owner};

    return $result;
}

sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr});
}

1;
