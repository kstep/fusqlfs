package FusqlFS::Base;

use strict;

use DBI;
use POSIX qw(mktime);

sub new {
    my $class = shift;
    my ($dsn, $options) = @_;

    my $self = {
	'table_info_cache' => {},
	'index_info_cache' => {},
	'fn_sep'           => $options->{'fn_sep'} || '.',
	'dsn'              => $dsn,
	'dbh'              => DBI->connect($dsn, $options->{'user'}, $options->{'password'}),
    };

    bless $self, $class;
    return $self;
}

sub DESTROY {
    $_[0]->{'dbh'}->disconnect();
}

# Table operations {{{

# @abstract
sub get_table_list {}

##
# Get table statistics
# @param string table
# @return hashref
# @abstract
sub get_table_stat {}

# @abstract
sub get_table_info {}

##
# Get create table clause
# @param string table
# @return string
# @abstract
sub get_create_table {}

# @abstract
sub create_table {}

sub drop_table {
    my $self = shift;
    if ($self->{'dbh'}->do("DROP TABLE $_[0]"))
    {
        $self->flush_table_cache($_[0]);
        return 1;
    }
    return 0;
}

sub rename_table {
    my $self = shift;
    $self->{'dbh'}->do("ALTER TABLE $_[0] RENAME TO $_[1]");
}

# }}}

# Index operations {{{

# @abstract
sub get_index_info {}

##
# Get fields included into primary key
# @param table
# @return list
# @abstract
sub get_primary_key {}

# @abstract
sub create_index {}

# @abstract
sub drop_index {}

sub modify_index {
    my $self = shift;
    my ($table, $index, $indexinfo) = @_;
    $self->drop_index($table, $index);
    $self->create_index($table, $index, $indexinfo);
}

# }}}

# Fields operations {{{

sub create_field {
    my $self = shift;
    if ($self->{'dbh'}->do("ALTER TABLE $_[0] ADD $_[1] int NOT NULL DEFAULT 0"))
    {
        $self->flush_table_cache($_[0]);
        return 1;
    }
    return 0;
}

# @abstract
sub modify_field {}

# @abstract
sub change_field {}

##
# Remove field from table
# @param string table
# @param string field
# @return bool
sub drop_field {
    my $self = shift;
    if ($self->{'dbh'}->do("ALTER TABLE $_[0] DROP $_[1]"))
    {
        $self->flush_table_cache($_[0]);
        return 1;
    }
    return 0;
}

# }}}

# Data operations {{{

# @abstract
sub get_table_data {}

###
# Get record from database
# @param string table
# @param hashref condition
# @param bool full
# @return list|hashref
sub get_record {
    my $self = shift;
    my ($table, $condition, $full) = @_;
    my @keys = keys %$condition;
    my $sql = "SELECT ". ($full? "*": join(',', @keys));
    $sql .= " FROM $table WHERE ". join(' AND ', map { "$_ = ?" } @keys);
    return wantarray? $self->{'dbh'}->selectrow_array($sql, undef, values %$condition): $self->{'dbh'}->selectrow_hashref($sql, undef, values %$condition);
}

sub insert_record {
    my $self = shift;
    my ($table, $record) = @_;
    my $sql = "INSERT INTO $table (";
    $sql .= join(',', keys %$record);
    $sql .= ") VALUES (". substr(',?' x scalar keys %$record, 1) .")";
    return $self->{'dbh'}->do($sql, undef, values %$record);
}

sub update_record {
    my $self = shift;
    my ($table, $condition, $record) = @_;
    my $sql = "UPDATE $table SET ";
    my @values = (values %$record, values %$condition);
    $sql .= join(',', map { "$_ = ?" } keys %$record);
    $sql .= " WHERE ". join(' AND ', map { "$_ = ?" } keys %$condition);
    return $self->{'dbh'}->do($sql, undef, @values);
}

sub save_record {
    my $self = shift;
    my ($table, $condition, $record) = @_;
    my $crecord = $self->get_record($table, $condition);
    if ($crecord) {
        $self->update_record($table, $condition, $record);
    } else {
        $self->insert_record($table, $record);
    }
}

sub delete_record {
    my $self = shift;
    my ($table, $record) = @_;
    my $sql = "DELETE FROM $table WHERE ";
    $sql .= join(' AND ', map { "$_ = ?" } keys %$record);
    return $self->{'dbh'}->do($sql, undef, values %$record);
}

# @abstract
sub create_record {}

# }}}

# Utility functions {{{

# @abstract
# @static
sub convert_field_to_sql {}

# @static
sub parse_sql_time {
    my $time = shift;
    my $result = 0;
    #              1 year  2 month 3 day   4 hour  5 min   6 sec
    if ($time =~ /(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})/) {
        $result = mktime($6, $5, $4, $3, $2 - 1, $1 - 1900);
    }
    return $result;
}

sub flush_table_cache {
    my $self = shift;
    delete $self->{'table_info_cache'}->{$_[0]};
    delete $self->{'index_info_cache'}->{$_[0]};
}

sub execute_queries {
    my $self = shift;
    my $buffer = shift;
    foreach (split /;\n/, $$buffer) {
        s/^\s+//; s/\s+$//;
        next unless $_;
        $self->{'dbh'}->do($_);
    }
}

sub execute_query {
    my $self = shift;
    my $query = shift;

    my $sth = $self->{'dbh'}->prepare($query);
    return -1 unless $sth->execute();

    my $buffer = $sth->fetchall_arrayref({});
    $sth->finish();
    return $buffer;
}

# }}}

1;
