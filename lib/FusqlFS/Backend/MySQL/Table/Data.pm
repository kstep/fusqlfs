use strict;
use 5.010;

package FusqlFS::Backend::MySQL::Table::Data;
our $VERSION = "0.005";
use parent 'FusqlFS::Backend::MySQL::Artifact';

=head1 NAME

FusqlFS::Backend::MySQL::Table::Data - 

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 EXPOSED STRUCTURE

=cut

sub init
{
    my $self = shift;

    $self->{get_primary_expr} = 'SHOW INDEX FROM `%s` WHERE Key_name = "PRIMARY"';
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    my @primary_key = $self->get_primary_key($table);
    return undef unless @primary_key;

    my $query = sprintf('SELECT %s FROM `%s` %s', $self->concat(@primary_key), $table, $self->limit);
    return $self->all_col($query)||[];
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my ($where_clause, @binds) = $self->where_clause($table, $name);
    return unless $where_clause || @binds;

    my $result = $self->one_row('SELECT * FROM `%s` WHERE %s LIMIT 1', [$table, $where_clause], @binds);

    return $self->dump($result);
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    my ($where_clause, @binds) = $self->where_clause($table, $name);
    return unless $where_clause || @binds;

    $self->cdo('DELETE FROM `%s` WHERE %s', [$table, $where_clause], @binds);
}

sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;
    my ($where_clause, @binds) = $self->where_clause($table, $name);
    return unless $where_clause || @binds;

    $data = $self->load($data);
    my $template = $self->pairs(', ', keys %$data);
    $self->cdo("UPDATE `%s` SET %s WHERE %s", [$table, $template, $where_clause], values %$data, @binds);
}

sub where_clause
{
    my $self = shift;
    my ($table, $name) = @_;
    my @binds = $self->asplit($name);
    my @primary_key = $self->get_primary_key($table);
    return unless @primary_key && $#primary_key == $#binds;

    return $self->pairs(' AND ', @primary_key), @binds;
}

sub get_primary_key
{
    my $self = shift;
    my ($table) = @_;
    my @result = ();
    my $data = $self->all_row($self->{get_primary_expr}, [$table]);
    if ($data)
    {
        @result = map { $_->{Column_name} } @$data;
    }
    return @result;
}

1;
