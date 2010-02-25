package FusqlFS::PgSQL;

use strict;

use DBI;

our @ISA;
require FusqlFS::Base;
@ISA = ('FusqlFS::Base');

our $fn_sep;

sub init {
    my $self = shift;
    my $options = shift;

    my $dsn = "DBI:pgsql:database=$options->{database}";
    $dsn .= ";host=$options->{host}" if ($options->{'host'});
    $dsn .= ";port=$options->{port}" if ($options->{'port'});
    $self->{'dsn'} = $dsn;
    $self->SUPER::init($options);

    $fn_sep = $options->{'fnsep'} || '.';
}

# Table operations {{{

sub get_table_list {
    my $self = shift;
}

sub get_table_stat {
    my $self = shift;
}

sub get_table_info {
    my $self = shift;
}

sub get_create_table {
    my $self = shift;
}

sub create_table {
    my $self = shift;
}

# }}}

# Index operations {{{

sub get_index_info {
    my $self = shift;
}

sub get_primary_key {
    my $self = shift;
}

sub create_index {
    my $self = shift;
}

sub drop_index {
    my $self = shift;
}

# }}}

# Fields operations {{{

sub modify_field {
    my $self = shift;
}

sub change_field {
    my $self = shift;
}

# }}}

# Data operations {{{

sub get_table_data {
    my $self = shift;
}

# }}}

# Utility functions {{{

# @static
sub convert_field_to_sql {

}

# }}}

new(__PACKAGE__);
