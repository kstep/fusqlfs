use strict;
use 5.010;

package FusqlFS::Backend::SQLite::Artifact;
use FusqlFS::Version;
our $VERSION = $FusqlFS::Version::VERSION;
use parent 'FusqlFS::Artifact';

sub init
{
    my $self = shift;
    $self->{type} = 'table';
    $self->{list_expr} = $self->expr('SELECT name FROM sqlite_master WHERE type = "%s"', $self->{type});
    $self->{get_expr} = $self->expr('SELECT sql FROM sqlite_master WHERE type = "%s" AND name = ?', $self->{type});
}

sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr});
}

sub get
{
    my $self = shift;
    my $table = shift;
    return $self->one_col($self->{get_expr}, $table);
}

1;
