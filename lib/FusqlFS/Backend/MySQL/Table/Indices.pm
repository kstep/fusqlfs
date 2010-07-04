use strict;
use 5.010;

package FusqlFS::Backend::MySQL::Table::Indices;
our $VERSION = "0.005";
use parent 'FusqlFS::Artifact';

=head1 NAME

FusqlFS::Backend::MySQL::Table::Indices - 

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 EXPOSED STRUCTURE

=cut

sub list
{
    my $self = shift;
    my ($table) = @_;
    my $data = $self->all_row('SHOW INDEX FROM `%s`', [$table]);
    return unless $data;

    my %keys = map { $_->{Key_name} => 1 } @$data;
    return [ keys %keys ];
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $data = $self->all_row('SHOW INDEX FROM `%s` WHERE `Key_name` = ?', [$table], $name);
    return unless $data;

    my $result = { '.order' => [] };
    foreach my $item (@$data)
    {
        my $colname = $item->{Sub_part}? $self->ajoin($item->{Column_name}, $item->{Sub_part}): $item->{Column_name};
        push @{$result->{'.order'}}, $item->{Column_name};

        $result->{$colname}   = \"tables/$table/struct/$item->{Column_name}";
        $result->{'.unique'}  = 1 unless $item->{Non_unique};
        $result->{'.type'}    = $item->{Index_type};
        $result->{'.primary'} = 1 if $item->{Key_name} =~ /^PRI/;
    }

    return $result;
}

1;

