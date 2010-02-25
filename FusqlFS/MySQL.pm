package FusqlFS::MySQL;

use strict;

use DBI;

our @ISA;
require FusqlFS::Base;
@ISA = ('FusqlFS::Base');

sub new {
    my ($class, $options) = @_;
    my $dsn = "DBI:mysql:database=$options->{database}";
    $dsn .= ";host=$options->{host}" if ($options->{'host'});
    $dsn .= ";port=$options->{port}" if ($options->{'port'});

    my $self = FusqlFS::Base::new($class, $dsn, $options);

    if ($options->{'charset'}) {
        my $def_charset = $options->{'charset'};
        $self->{'def_charset'} = $def_charset;
        $self->{'dbh'}->do("SET character_set_results = $def_charset");
        $self->{'dbh'}->do("SET character_set_client = $def_charset");
        $self->{'dbh'}->do("SET character_set_connection = $def_charset");
    }

    $self->{'def_engine'} = $options->{'useinnodb'}? 'InnoDB': 'MyISAM';

    $self->{'base_rtxtsz'} = {};
    $self->{'base_itxtsz'} = { qw(decimal 95 set 63 enum 63 float 46 text 46) };
    $self->{'base_stxtsz'} = 234;
    $self->{'def_base_itxtsz'} = 55;

    return $self;
}

# Table operations {{{

sub get_table_list {
    my $self = shift;
    my $result = $self->{'dbh'}->selectcol_arrayref("SHOW TABLES") || [];
    return @$result;
}

##
# Get table statistics
# @param string table
# @return hashref
sub get_table_stat {
    my $self = shift;
    my $table = shift;

    my %result;
    my $sth = $self->{'dbh'}->prepare("SHOW TABLE STATUS".(defined $table && " LIKE '$table'"));

    $sth->execute();
    while (my @row = $sth->fetchrow_array()) {
        $result{$row[0]} = {
            'Engine'          => $row[1],
            'Version'         => $row[2],
            'Row_format'      => $row[3],
            'Rows'            => 0 + $row[4],
            'Avg_row_length'  => 0 + $row[5],
            'Data_length'     => 0 + $row[6],
            'Max_data_length' => 0 + $row[7],
            'Index_length'    => 0 + $row[8],
            'Data_free'       => 0 + $row[9],
            'Auto_increment'  => 0 + $row[10],
            'Create_time'     => FusqlFS::Base::parse_sql_time($row[11]),
            'Update_time'     => FusqlFS::Base::parse_sql_time($row[12]),
            'Check_time'      => FusqlFS::Base::parse_sql_time($row[13]),
            'Collation'       => $row[14],
            'Checksum'        => $row[15],
            'Create_options'  => $row[16],
            'Comment'         => $row[17],
        };
    }
    $sth->finish();

    return $table? $result{$table}: \%result;
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

    my $sth = $self->{'dbh'}->prepare("SHOW FULL COLUMNS FROM $table");

    my %result;

    $sth->execute();
    $self->{'base_rtxtsz'}->{$table} = 4;
    while (my @row = $sth->fetchrow_array()) {
        $self->{'base_rtxtsz'}->{$table} += 3 + length($row[0]);
        next if $field && $row[0] ne $field;
        $result{$row[0]} = {
            'Collation'  => $row[2],
            'Not_null'   => $row[3] eq 'NO' || 0, # 7
            'Key'        => $row[4], # 6
            'Default'    => $row[5], # 10
            'Extra'      => $row[6],  # 8
            'Privileges' => [ split(/,/, $row[7]) ],
            'Comment'    => $row[8],
        };
        my ($type, $info) = (split /\(/, $row[1], 2);
        $result{$row[0]}->{'Type'} = $type;
        if ($type eq 'decimal') {
            my ($length, $decimal) = split /,/, $info;
            $result{$row[0]}->{'Length'} = 0 + $length;
            $result{$row[0]}->{'Decimal'} = 0 + $decimal;
            $result{$row[0]}->{'Zerofill'} = $row[1] =~ /zerofill/ || 0;
            $result{$row[0]}->{'Unsigned'} = $row[1] =~ /unsigned/ || 0;
        } elsif ($type eq 'set' || $type eq 'enum') {
            $result{$row[0]}->{'Enum'} = [ map { s/''/'/g; $_ } split(/','/, substr($info, 1, -2)) ];
        } elsif ($info) {
            $result{$row[0]}->{'Length'} = 0 + $info;
        }
    }
    $sth->finish();

    $self->{'table_info_cache'}->{$table} = \%result unless $field;
    return wantarray? keys %result: ($field? $result{$field}: \%result);
}

##
# Get create table clause
# @param string table
# @return string
sub get_create_table {
    my $self = shift;
    my @row = $self->{'dbh'}->selectrow_array("SHOW CREATE TABLE $_[0]");
    return @row? $row[1]: undef;
}

sub create_table {
    my $self = shift;
    return $self->{'dbh'}->do("CREATE TABLE $_[0] ($_[1] int NOT NULL auto_increment, PRIMARY KEY (id))".($self->{'def_engine'} && " ENGINE=$self->{def_engine}").($self->{'def_charset'} && " DEFAULT CHARSET=$self->{def_charset}"));
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
    my $sth = $self->{'dbh'}->prepare("SHOW INDEX FROM $table");

    $sth->execute();
    while (my @row = $sth->fetchrow_array()) {
        next if $index && $row[2] ne $index;
        if (exists $result{$row[2]}) {
            push @{ $result{$row[2]}->{'Column_name'} }, $row[4].($row[7] && "$self->{fn_sep}$row[7]");
        } else {
            $result{$row[2]} = {
                'Unique'      => !$row[1] || 0,
                'Column_name' => [ $row[4].($row[7] && "$self->{fn_sep}$row[7]") ],
                'Collation'   => $row[5],
                'Cardinality' => 0 + $row[6],
                'Packed'      => $row[8],
                'Not_null'    => !$row[9] || 0,
                'Index_type'  => $row[10],
                'Comment'     => $row[11],
            };
        }
    }
    $sth->finish();

    $self->{'index_info_cache'}->{$table} = \%result unless $index;

    return wantarray? ($index? @{ $result{$index}->{'Column_name'} || [] }: keys %result): ($index? $result{$index}: \%result);
}

##
# Get fields included into primary key
# @param table
# @return list
sub get_primary_key {
    my $self = shift;
    my $table = shift;
    my $tableinfo = $self->get_table_info($table);
    return grep $tableinfo->{$_}->{'Key'} eq 'PRI', sort keys %$tableinfo;
}

sub create_index {
    my $self = shift;
    my ($table, $index, $idesc) = @_;
    my @fields = @{ $idesc->{'Column_name'} };
    my $index = $index =~ /^PRI/? 'PRIMARY KEY': ($idesc->{'Unique'}? 'UNIQUE ':'')."KEY $index";
    my $sql = "ALTER TABLE $table ADD $index (";
    $sql .= join(',', map { my ($name, $part) = split /$self->{fn_sep}/, $_; $part += 0; $part? "$name($part)": $name } @fields);
    $sql .= ")";
    return $self->{'dbh'}->do($sql);
}

sub drop_index {
    my $self = shift;
    my ($table, $index) = @_;
    my $index = $index =~ /^PRI/? 'PRIMARY KEY': "KEY $index";
    if ($self->{'dbh'}->do("ALTER TABLE $table DROP $index"))
    {
        delete $self->{'index_info_cache'}->{$table}->{$index};
        return 1;
    }
    return 0;
}

# }}}

# Data operations {{{

sub get_table_data {
    my $self = shift;
    my $table = shift;
    my @keys = $self->get_primary_key($table);
    return () unless @keys;
    my $result = $self->{'dbh'}->selectcol_arrayref("SELECT CONCAT_WS('$self->{fn_sep}',".join(',',@keys).") FROM $table") || [];
    return @$result;
}

sub create_record {
    my $self = shift;
    my ($table, $name) = @_;
    my $tableinfo = $self->get_table_info($table);
    my %record;

    unless ($name eq 'auto') {
        my @keys = grep $tableinfo->{$_}->{'Key'} eq 'PRI',
        sort keys %$tableinfo;
        my @values = split /$self->{fn_sep}/, $name;
        my $i = 0;
        %record = map { $_ => $values[$i++] } @keys;
    }

    while (my ($key, $field) = each %$tableinfo) {
        next unless $field->{'Not_null'} && $field->{'Default'} eq '';
        next if $field->{'Extra'} =~ /auto_increment/;

        if ($field->{'Type'} eq 'set' || $field->{'Type'} eq 'enum')
        {
            $record{$key} = $field->{'Enum'}->[0];
        } elsif ($field->{'Type'} eq 'float' || $field->{'Type'} eq 'decimal'
            || $field->{'Type'} =~ /int/)
        {
            $record{$key} = 0;
        } else {
            $record{$key} = '';
        }
    }

    return $self->insert_record($table, \%record);

}

# }}}

# Fields operations {{{

sub modify_field {
    my $self = shift;
    my ($table, $field, $fdesc) = @_;
    my ($sql, @values) = convert_field_to_sql($fdesc);
    print STDERR "ALTER TABLE $table MODIFY $field $sql\n";
    return $self->{'dbh'}->do("ALTER TABLE $table MODIFY $field $sql", undef, @values);
}

sub change_field {
    my $self = shift;
    my ($table, $field, $nfield, $fdesc) = @_;
    $fdesc ||= $self->get_table_info($table, $field);
    my ($sql, @values) = convert_field_to_sql($fdesc);
    print STDERR "ALTER TABLE $table CHANGE $field $nfield $sql\n";
    return $self->{'dbh'}->do("ALTER TABLE $table CHANGE $field $nfield $sql", undef, @values);
}

# }}}

# Utility functions {{{

sub convert_field_to_sql {
    my $fdesc = shift;
    return unless ref($fdesc) eq "HASH";
    my $sql = $fdesc->{'Type'};
    my @values;
    if ($fdesc->{'Type'} eq 'enum' || $fdesc->{'Type'} eq 'set') {
        @values = @{ $fdesc->{'Enum'} };
        $sql .= "(".substr(',?' x scalar @values, 1).")";
    } elsif ($fdesc->{'Type'} eq 'decimal') {
        $sql .= "($fdesc->{Length},$fdesc->{Decimal})";
        $sql .= " unsigned" if $fdesc->{'Unsigned'};
        $sql .= " zerofill" if $fdesc->{'Zerofill'};
    } elsif ($fdesc->{'Length'}) {
        $sql .= "($fdesc->{Length})";
    }
    $sql .= " NOT NULL" if $fdesc->{'Not_null'};
    if ($fdesc->{'Default'}) {
        $sql .= " DEFAULT ?";
        push @values, $fdesc->{'Default'};
    }
    $sql .= " $fdesc->{Extra}" if $fdesc->{'Extra'};
    return ($sql, @values);
}

# }}}

1;
