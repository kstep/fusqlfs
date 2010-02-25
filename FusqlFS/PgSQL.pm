package FusqlFS::PgSQL;

use strict;

use DBI;

our @ISA;
require FusqlFS::Base;
@ISA = ('FusqlFS::Base');

our $fn_sep;

sub new {
    my ($class, $options) = @_;
    my $dsn = "DBI:Pg:database=$options->{database}";
    $dsn .= ";host=$options->{host}" if ($options->{'host'});
    $dsn .= ";port=$options->{port}" if ($options->{'port'});

    my $self = FusqlFS::Base::new($class, $dsn, $options);

    return $self;
}

# Table operations {{{

sub get_table_list {
    my $self = shift;
    my $result = $self->{'dbh'}->selectcol_arrayref("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'") || [];
    return @$result;
}

sub get_table_stat {
    my $self = shift;
    return {};
}

sub get_table_info {
    my $self = shift;
    return {};
}

sub get_create_table {
    my $self = shift;
    return "";
}

sub create_table {
    my $self = shift;
}

# }}}

# Index operations {{{

sub get_index_info {
    my $self = shift;
    return {};
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
    return {};
}

# }}}

# Utility functions {{{

# @static
sub convert_field_to_sql {
    return "";
}

# }}}

1;
