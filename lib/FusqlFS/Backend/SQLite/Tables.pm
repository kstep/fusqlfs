use strict;
use 5.010;

package FusqlFS::Backend::SQLite::Tables;
use FusqlFS::Version;
our $VERSION = $FusqlFS::Version::VERSION;
use parent 'FusqlFS::Artifact';

sub init
{
    my $self = shift;
    $self->{list_expr} = $self->expr('SELECT name FROM sqlite_master WHERE type = "table"');
    $self->{get_expr} = $self->expr('SELECT sql, rootpage FROM sqlite_master WHERE type = "table" AND name = ?');
    $self->{rename_expr} = 'ALTER TABLE %s RENAME TO %s';
    $self->{drop_expr} = 'DROP TABLE %s';
    $self->{create_expr} = 'CREATE TABLE %s (id INT)';

    $self->autopackages('indices', 'data', 'struct');
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

    $self->extend($data, $self->{subpackages});
    $data->{sql} ||= '';
    return $data;
}

sub rename
{
    my $self = shift;
    my ($table, $newtable) = @_;
    $self->do($self->{rename_expr}, [$table, $newtable]);
}

sub drop
{
    my $self = shift;
    my ($table) = @_;
    $self->do($self->{drop_expr}, [$table]);
}

sub create
{
    my $self = shift;
    my ($table) = @_;
    $self->do($self->{create_expr}, [$table]);
}

1;
