package FusqlFS::PgSQL;

use strict;

use DBI;
use Data::Dump qw(dump ddx);

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
    my ($table, $field) = @_;

    if (exists $self->{'table_info_cache'}->{$table} && %{ $self->{'table_info_cache'}->{$table} }) {

        if (wantarray) {
            return $field? exists $self->{'table_info_cache'}->{$table}->{$field}?($field):(): keys %{ $self->{'table_info_cache'}->{$table} };
        } else {
            return $field? $self->{'table_info_cache'}->{$table}->{$field}:
            $self->{'table_info_cache'}->{$table};
        }
    }

    my $sth = $self->{'dbh'}->prepare("SELECT attname, typname as Type, pg_catalog.format_type(atttypid, atttypmod) AS Type_name, attnotnull as Not_null,
	    CASE WHEN atthasdef THEN
		(SELECT pg_catalog.pg_get_expr(adbin, adrelid) FROM pg_attrdef as d
		    WHERE adrelid = attrelid AND adnum = attnum)
	    ELSE NULL END AS Default,
	    CASE WHEN atttypmod < 0 THEN NULL
		WHEN typcategory = 'N' THEN (((atttypmod-4)>>16)&65535)
		ELSE atttypmod-4 END AS Length,
	    CASE WHEN atttypmod < 0 THEN NULL
		WHEN typcategory = 'N' THEN ((atttypmod-4)&65535)
		ELSE NULL END AS Decimal,
	    attndims AS Dimensions,
	    attnum as Order
	FROM pg_catalog.pg_attribute as a, pg_catalog.pg_type as t
	WHERE a.atttypid = t.oid AND attrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')
	    AND attnum > 0
	ORDER BY attnum");

    my %result;
    $sth->execute($table);
    while (my @row = $sth->fetchrow_array()) {
	next if $field && $row[0] ne $field;
	$row[2] =~ s/[\(\[].*//;
	$result{$row[0]} = {
	    'Type'       => $row[2],
	    'Not_null'   => $row[3],
	    'Default'    => $row[4],
	    'Length'     => $row[5],
	    'Decimal'    => $row[6],
	    'Dimensions' => $row[7],
	    'Order'      => $row[8],
	};
    }
    $sth->finish();

    $self->{'table_info_cache'}->{$table} = \%result unless $field;
    return wantarray? keys %result: ($field? $result{$field}: \%result);
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
    my ($table, $index) = @_;

    if (exists $self->{'index_info_cache'}->{$table}->{$index}) {
        if (wantarray) {
            return $index? @{ $self->{'index_info_cache'}->{$table}->{$index}->{'Column_name'} }: keys %{ $self->{'index_info_cache'}->{$table} };
        } else {
            return $index? $self->{'index_info_cache'}->{$table}->{$index}: $self->{'index_info_cache'}->{$table};
        }
    }

    my %result;
    my $sth = $self->{'dbh'}->prepare("SELECT (SELECT c1.relname FROM pg_catalog.pg_class as c1 WHERE c1.oid = indexrelid) as Index_name,
	    pg_get_indexdef(indexrelid, 0, true) AS Index_expr,
	    indisunique as Is_unique, indisprimary as Is_primary, indkey as Fields
	FROM pg_catalog.pg_index
	    WHERE indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r');");

    $sth->execute($table);
    while (my @row = $sth->fetchrow_array()) {
	next if $index && $row[0] ne $index;
	dump(\@row);

	$result{$row[0]} = {
	    'Expression'  => $row[1],
	    'Unique'      => $row[2],
	    'Primary'     => $row[3],
	};
	if ($row[4]) {
	    my $tableinfo = $self->get_table_info($table);
	    my @tableinfo = sort { $tableinfo->{$a}->{'Order'} <=> $tableinfo->{$b}->{'Order'} } keys %$tableinfo;
	    $result{$row[0]}->{'Column_name'} = [ map { $tableinfo[$_-1] } split(/ /, $row[4]) ];
	} else {
	    $result{$row[0]}->{'Column_name'} = [];
	}
    }
    $sth->finish();

    $self->{'index_info_cache'}->{$table} = \%result unless $index;
    return wantarray? ($index? @{ $result{$index}->{'Column_name'} || [] }: keys %result): ($index? $result{$index}: \%result);
}

sub get_primary_key {
    my $self = shift;
    my $table = shift;
    my $result = $self->{'dbh'}->selectcol_arrayref("SELECT attname FROM pg_catalog.pg_attribute, pg_catalog.pg_index
	WHERE attrelid = indexrelid AND indrelid = (SELECT oid FROM pg_catalog.pg_class as c WHERE c.relname = ? AND relkind = 'r')
	    AND attnum > 0 AND indisprimary
	ORDER BY attnum", {}, $table);
    return @$result;
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
    my $table = shift;
    my @keys = $self->get_primary_key($table);
    return () unless @keys;
    my @result = map { join($self->{'fn_sep'}, @$_) } @{$self->{'dbh'}->selectall_arrayref("SELECT ".join(',',@keys)." FROM $table") || []};
    print STDERR @result;
    return @result;
}

# }}}

# Utility functions {{{

# @static
sub convert_field_to_sql {
    return "";
}

# }}}

1;
