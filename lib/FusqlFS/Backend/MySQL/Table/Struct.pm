use strict;
use 5.010;

package FusqlFS::Backend::MySQL::Table::Struct;
our $VERSION = "0.005";
use base 'FusqlFS::Artifact';

=head1 NAME

FusqlFS::Backend::MySQL::Table::Struct

=head1 SYNOPSIS

    Synopsis

=head1 DESCRIPTION

Description

=cut

sub init
{
    my $self = shift;

    $self->{list_expr} = 'SHOW COLUMNS FROM `%s`';
    $self->{get_expr} = 'SHOW FULL COLUMNS FROM `%s` LIKE "%s"';
}

=begin testing list

is $_tobj->list('unknown'), undef, 'Unknown table';
cmp_set $_tobj->list('fusqlfs_table'), ['id'], 'Test table listable';

=end testing
=cut
sub list
{
    my $self = shift;
    my ($table) = @_;
    my $list = $self->all_col($self->{list_expr}, [$table]);
    return unless $list && @$list;
    return $list
}

=begin testing get

is $_tobj->get('fusqlfs_table', 'unknown'), undef, 'Unknown field';
is $_tobj->get('fusqlfs_table', 'id'), q{---
collation: ~
comment: ''
default: ~
extra: auto_increment
key: PRI
null: 0
privileges:
  - select
  - insert
  - update
  - references
type: int(11)
};

=end testing
=cut
sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $result = $self->one_row($self->{get_expr}, [$table, $name]);
    if ($result)
    {
        $result = { map { lc($_) => $result->{$_} } grep { $_ ne 'Field' } keys %$result };
        $result->{privileges} = [ split /,/, $result->{privileges} ];
        $result->{null} = $result->{null} eq 'YES'? 1: 0;
        return $self->dump($result);
    }
    return;
}

=begin testing drop after rename

isnt $_tobj->drop('fusqlfs_table', 'new_field'), undef, 'Field is dropped';
is $_tobj->get('fusqlfs_table', 'new_field'), undef, 'Field is not gettable';
is_deeply $_tobj->list('fusqlfs_table'), ['id'], 'Field is not listable';

=end testing
=cut
sub drop
{
    my $self = shift;
    #body ...
}

=begin testing create after get list

#test body ...

=end testing
=cut
sub create
{
    my $self = shift;
    #body ...
}

=begin testing rename after store

#test body ...

=end testing
=cut
sub rename
{
    my $self = shift;
    #body ...
}

=begin testing store after create

#test body ...

=end testing
=cut
sub store
{
    my $self = shift;
    #body ...
}
1;

__END__

=begin testing SETUP

#!class FusqlFS::Backend::MySQL::Table::Test

=end testing
=cut
