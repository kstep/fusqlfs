use strict;
use 5.010;

package FusqlFS::Backend::SQLite::Tables;
use FusqlFS::Version;
our $VERSION = $FusqlFS::Version::VERSION;
use parent 'FusqlFS::Artifact';

use FusqlFS::Backend::SQLite::Table::Indices;

sub init
{
    my $self = shift;
    $self->{list_expr} = $self->expr('SELECT name FROM sqlite_master WHERE type = "table"');
    $self->{get_expr} = $self->expr('SELECT sql, rootpage FROM sqlite_master WHERE type = "table" AND name = ?');

    $self->{subpackages} = {
        indices => new FusqlFS::Backend::SQLite::Table::Indices(),
    };
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
    my $data = $self->one_row($self->{get_expr}, $table);
    return unless $data;

    $data->{indices} = $self->{subpackages}->{indices};
    $data->{sql} ||= '';
    return $data;
}

1;
