package FusqlFS::MySQL;

use strict;

use DBI;
use POSIX qw(mktime);

require Exporter;
our (@ISA, @EXPORT);

@ISA = qw(Exporter);

BEGIN {
    @EXPORT = qw(
    change_field
    create_field
    create_index
    create_table
    delete_record
    drop_field
    drop_index
    drop_table
    execute_queries
    flush_table_cache
    get_create_table
    get_index_info
    get_primary_key
    get_record
    get_table_data
    get_table_info
    get_table_list
    get_table_stat
    init_db
    insert_record
    modify_field
    modify_index
    rename_table
    save_record
    update_record
    );
}

our $dbh;
our $def_engine;
our $def_charset;
our $fn_sep;

our %table_info_cache = ();
our %index_info_cache = ();

our %base_rtxtsz = ();
our %base_itxtsz = qw(decimal 95 set 63 enum 63 float 46 text 46);
our $base_stxtsz = 234;
our $def_base_itxtsz = 55;

sub init_db {
    my $options = shift;

    my $dsn = "DBI:mysql:database=$options->{database}";
    $dsn .= ";host=$options->{host}" if ($options->{'host'});
    $dsn .= ";port=$options->{port}" if ($options->{'port'});
    $dbh = DBI->connect($dsn, $options->{'user'}, $options->{'password'});

    if ($options->{'charset'}) {
        $def_charset = $options->{'charset'};
        $dbh->do("SET character_set_results = $def_charset");
        $dbh->do("SET character_set_client = $def_charset");
        $dbh->do("SET character_set_connection = $def_charset");
    }

    $def_engine = $options->{'useinnodb'}? 'InnoDB': 'MyISAM';
    $fn_sep = $options->{'fnsep'} || '.';
}

sub DESTROY {
    $dbh->disconnect();
}

# Table operations {{{

sub get_table_list {
    my $result = $dbh->selectcol_arrayref("SHOW TABLES") || [];
    return @$result;
}

##
# Get table statistics
# @param string table
# @return hashref
sub get_table_stat {
    my $table = shift;

    my %result;
    my $sth = $dbh->prepare("SHOW TABLE STATUS".(defined $table && " LIKE '$table'"));

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
            'Create_time'     => parse_sql_time($row[11]),
            'Update_time'     => parse_sql_time($row[12]),
            'Check_time'      => parse_sql_time($row[13]),
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
    my ($table, $field) = @_;

    if (exists $table_info_cache{$table} && %{ $table_info_cache{$table} }) {

        if (wantarray) {
            return $field? exists $table_info_cache{$table}->{$field}?($field):(): keys %{ $table_info_cache{$table} };
        } else {
            return $field? $table_info_cache{$table}->{$field}:
            $table_info_cache{$table};
        }
    }

    my $sth = $dbh->prepare("SHOW FULL COLUMNS FROM $table");

    my %result;

    $sth->execute();
    $base_rtxtsz{$table} = 4;
    while (my @row = $sth->fetchrow_array()) {
        $base_rtxtsz{$table} += 3 + length($row[0]);
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

    $table_info_cache{$table} = \%result unless $field;
    return wantarray? keys %result: ($field? $result{$field}: \%result);
}

##
# Get create table clause
# @param string table
# @return string
sub get_create_table {
    my @row = $dbh->selectrow_array("SHOW CREATE TABLE $_[0]");
    return @row? $row[1]: undef;
}

sub create_table {
    return $dbh->do("CREATE TABLE $_[0] ($_[1] int NOT NULL auto_increment, PRIMARY KEY (id))".($def_engine && " ENGINE=$def_engine").($def_charset && " DEFAULT CHARSET=$def_charset"));
}

sub drop_table {
    if ($dbh->do("DROP TABLE $_[0]"))
    {
        flush_table_cache($_[0]);
        return 1;
    }
    return 0;
}

sub rename_table {
    $dbh->do("ALTER TABLE $_[0] RENAME TO $_[1]");
}
# }}}

# Index operations {{{

sub get_index_info {
    my ($table, $index) = @_;

    if (exists $index_info_cache{$table}->{$index}) {
        if (wantarray) {
            return $index? @{ $index_info_cache{$table}->{$index}->{'Column_name'} }: keys %{ $index_info_cache{$table} };
        } else {
            return $index? $index_info_cache{$table}->{$index}: $index_info_cache{$table};
        }
    }

    my %result;
    my $sth = $dbh->prepare("SHOW INDEX FROM $table");

    $sth->execute();
    while (my @row = $sth->fetchrow_array()) {
        next if $index && $row[2] ne $index;
        if (exists $result{$row[2]}) {
            push @{ $result{$row[2]}->{'Column_name'} }, $row[4].($row[7] && "$fn_sep$row[7]");
        } else {
            $result{$row[2]} = {
                'Unique'      => !$row[1] || 0,
                'Column_name' => [ $row[4].($row[7] && "$fn_sep$row[7]") ],
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

    $index_info_cache{$table} = \%result unless $index;

    return wantarray? ($index? @{ $result{$index}->{'Column_name'} || [] }: keys %result): ($index? $result{$index}: \%result);
}

##
# Get fields included into primary key
# @param table
# @return list
sub get_primary_key {
    my $table = shift;
    my $tableinfo = get_table_info($table);
    return grep $tableinfo->{$_}->{'Key'} eq 'PRI', sort keys %$tableinfo;
}

sub create_index {
    my ($table, $index, $idesc) = @_;
    my @fields = @{ $idesc->{'Column_name'} };
    my $index = $index =~ /^PRI/? 'PRIMARY KEY': ($idesc->{'Unique'}? 'UNIQUE ':'')."KEY $index";
    my $sql = "ALTER TABLE $table ADD $index (";
    $sql .= join(',', map { my ($name, $part) = split /$fn_sep/, $_; $part += 0; $part? "$name($part)": $name } @fields);
    $sql .= ")";
    return $dbh->do($sql);
}

sub drop_index {
    my ($table, $index) = @_;
    my $index = $index =~ /^PRI/? 'PRIMARY KEY': "KEY $index";
    if ($dbh->do("ALTER TABLE $table DROP $index"))
    {
        delete $index_info_cache{$table}->{$index};
        return 1;
    }
    return 0;
}

sub modify_index {
    my ($table, $index, $indexinfo) = @_;
    drop_index($table, $index);
    create_index($table, $index, $indexinfo);
}

# }}}

# Data operations {{{

sub get_table_data {
    my $table = shift;
    my @keys = get_primary_key($table);
    return () unless @keys;
    my $result = $dbh->selectcol_arrayref("SELECT CONCAT_WS('$fn_sep',".join(',',@keys).") FROM $table") || [];
    return @$result;
}

###
# Get record from database
# @param string table
# @param hashref condition
# @param bool full
# @return list|hashref
sub get_record {
    my ($table, $condition, $full) = @_;
    my @keys = keys %$condition;
    my $sql = "SELECT ". ($full? "*": join(',', @keys));
    $sql .= " FROM $table WHERE ". join(' AND ', map { "$_ = ?" } @keys);
    return wantarray? $dbh->selectrow_array($sql, undef, values %$condition): $dbh->selectrow_hashref($sql, undef, values %$condition);
}

sub insert_record {
    my ($table, $record) = @_;
    my $sql = "INSERT INTO $table (";
    $sql .= join(',', keys %$record);
    $sql .= ") VALUES (". substr(',?' x scalar keys %$record, 1) .")";
    return $dbh->do($sql, undef, values %$record);
}

sub update_record {
    my ($table, $condition, $record) = @_;
    my $sql = "UPDATE $table SET ";
    my @values = (values %$record, values %$condition);
    $sql .= join(',', map { "$_ = ?" } keys %$record);
    $sql .= " WHERE ". join(' AND ', map { "$_ = ?" } keys %$condition);
    return $dbh->do($sql, undef, @values);
}

sub save_record {
    my ($table, $condition, $record) = @_;
    my $crecord = get_record($table, $condition);
    if ($crecord) {
        update_record($table, $condition, $record);
    } else {
        insert_record($table, $record);
    }
}

sub delete_record {
    my ($table, $record) = @_;
    my $sql = "DELETE FROM $table WHERE ";
    $sql .= join(' AND ', map { "$_ = ?" } keys %$record);
    return $dbh->do($sql, undef, values %$record);
}

# }}}

# Fields operations {{{

sub create_field {
    if ($dbh->do("ALTER TABLE $_[0] ADD $_[1] int NOT NULL DEFAULT 0"))
    {
        flush_table_cache($_[0]);
        return 1;
    }
    return 0;
}

sub modify_field {
    my ($table, $field, $fdesc) = @_;
    my ($sql, @values) = convert_field_to_sql($fdesc);
    print STDERR "ALTER TABLE $table MODIFY $field $sql\n";
    return $dbh->do("ALTER TABLE $table MODIFY $field $sql", undef, @values);
}

sub change_field {
    my ($table, $field, $nfield, $fdesc) = @_;
    $fdesc ||= get_table_info($table, $field);
    my ($sql, @values) = convert_field_to_sql($fdesc);
    print STDERR "ALTER TABLE $table CHANGE $field $nfield $sql\n";
    return $dbh->do("ALTER TABLE $table CHANGE $field $nfield $sql", undef, @values);
}

##
# Remove field from table
# @param string table
# @param string field
# @return bool
sub drop_field {
    if ($dbh->do("ALTER TABLE $_[0] DROP $_[1]"))
    {
        flush_table_cache($_[0]);
        return 1;
    }
    return 0;
}

# }}}

# Utility functions {{{

sub parse_sql_time {
    my $time = shift;
    my $result = 0;
    #              1 year  2 month 3 day   4 hour  5 min   6 sec
    if ($time =~ /(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})/) {
        $result = mktime($6, $5, $4, $3, $2 - 1, $1 - 1900);
    }
    return $result;
}

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

sub flush_table_cache {
    delete $table_info_cache{$_[0]};
    delete $index_info_cache{$_[0]};
}

sub execute_queries {
    my $buffer = shift;
    foreach (split /;\n/, $$buffer) {
        s/^\s+//; s/\s+$//;
        next unless $_;
        $dbh->do($_);
    }
}

# }}}

1;
