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

    $self->{create_expr} = 'ALTER TABLE `%s` ADD COLUMN `%s` INT NOT NULL DEFAULT 0';
    $self->{rename_expr} = 'ALTER TABLE `%s` CHANGE COLUMN `%s` `%s` %s';
    $self->{drop_expr} = 'ALTER TABLE `%s` DROP COLUMN `%s`';
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
    my ($table, $name) = @_;
    $self->do($self->{drop_expr}, [$table, $name]);
}

=begin testing create after get list

isnt $_tobj->create('fusqlfs_table', 'field'), undef, 'Create field';
is $_tobj->get('fusqlfs_table', 'field'), $new_field, 'New field exists';
is_deeply $_tobj->list('fusqlfs_table'), ['id', 'field'], 'New field is listable';

=end testing
=cut
sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->do($self->{create_expr}, [$table, $name]);
}

=begin testing rename after create

isnt $_tobj->rename('fusqlfs_table', 'field', 'new_field'), undef, 'Field renamed';
is $_tobj->get('fusqlfs_table', 'field'), undef, 'New field is unaccessible by old name';
is $_tobj->get('fusqlfs_table', 'new_field'), $new_field, 'New field exists';
is_deeply $_tobj->list('fusqlfs_table'), ['id', 'new_field'], 'New field is listable';

=end testing
=cut
sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    my $field = $self->one_row($self->{get_expr}, [$table, $name]);
    return unless $field;
    my $fielddef = sprintf('%s %s NULL DEFAULT %s %s',
                            $field->{Type},
                            $field->{Null} eq 'YES'? '': 'NOT',
                            $field->{Default},
                            $field->{Extra});
    $self->do($self->{rename_expr}, [$table, $name, $newname, $fielddef]);
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

my $new_field = q{---
collation: ~
comment: ''
default: 0
extra: ''
key: ''
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
