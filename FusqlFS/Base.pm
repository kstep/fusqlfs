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

    $self->{'.mods'} = { map { $pkg = $class.'::'.$_; lc $_ => $pkg->new($self); } qw(Tables Views Roles Functions Queries) }
    return $self;
}

# @param string table
# @action flush table cache for given table
sub flush_table_cache {
    my $self = shift;
    delete $self->{'table_info_cache'}->{$_[0]};
    delete $self->{'index_info_cache'}->{$_[0]};
}


sub DESTROY {
    $_[0]->{'dbh'}->disconnect();
}

1;

# Base module {{{
package FusqlFS::Base::Module;
use strict;

sub new {
    $class = shift;
    $parent = shift;
    my $self = { 'parent' => $parent, 'dbh' => $parent->{'dbh'}, 'fn_sep' => $parent->{'fn_sep'}, };
    bless $self, $class;
    return $self;
}

# @abstract
# @param string table
# @return list of tables
sub list {}


# @abstract
# @param string table
# @return hashref table info
sub get {}

# @abstract
# @param string table
# @return string create table clause
sub get_sql {}

# @abstract
# @param string table
# @param optional hashref table info
# @action create table
sub create {}

# @abstract
# @param string table
# @action drop table
sub drop {}

# @param string table
# @param string new name
# @action rename table
sub rename {}

1;
# }}}

# Table operations {{{
package FusqlFS::Base::Tables;
use strict;
our @ISA = ('FusqlFS::Base::Module');

sub new {
    my $self = SUPER::new(@_);
    $self->{'table_info_cache'} = {};
    $self->{'index_info_cache'} = {};
    return $self;
}

sub drop {
    my $self = shift;
    if ($self->{'dbh'}->do("DROP TABLE $_[0]"))
    {
        $self->{'parent'}->flush_table_cache($_[0]);
        return 1;
    }
    return 0;
}

sub rename {
    my $self = shift;
    $self->{'dbh'}->do("ALTER TABLE $_[0] RENAME TO $_[1]");
}

# @abstract
# @param string table
# @return hashref table stats
sub get_stats {}

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

1;
# }}}

# Index operations {{{
package FusqlFS::Base::Indices;
use strict;
our @ISA = ('FusqlFS::Base::Module');

sub store {
    my $self = shift;
    my ($index, $indexinfo) = @_;
    $self->drop($self->{'table'}, $index);
    $self->create($self->{'table'}, $index, $indexinfo);
}

1;
# }}}

# Fields operations {{{
package FusqlFS::Base::Struct;
use strict;
our @ISA = ('FusqlFS::Base::Module');

sub create {
    my $self = shift;
    if ($self->{'dbh'}->do("ALTER TABLE $self->{table} ADD $_[0] int NOT NULL DEFAULT 0"))
    {
        $self->{'parent'}->{'parent'}->flush_table_cache($self->{'table'});
        return 1;
    }
    return 0;
}

sub drop {
    my $self = shift;
    if ($self->{'dbh'}->do("ALTER TABLE $self->{table} DROP $_[0]"))
    {
        $self->{'parent'}->{'parent'}->flush_table_cache($self->{'table'});
        return 1;
    }
    return 0;
}

# @abstract
# @static
sub convert_field_to_sql {}

1;
# }}}

# Data operations {{{
package FusqlFS::Base::Data;
use strict;
our @ISA = ('FusqlFS::Base::Module');

# @abstract
# @return list primary key fields
sub get_primary_key {}

sub get {
    my $self = shift;
    my $cond = $self->map_name_to_rec($_[0]);
    return $self->get_record($cond, 1) if $cond;
}

sub list {
    my $self = shift;
    my @keys = $self->get_primary_key();
    return () unless @keys;
    my @result = map { join($self->{'fn_sep'}, @$_) } @{$self->{'dbh'}->selectall_arrayref("SELECT ".join(',',@keys)." FROM $self->{table}") || []};
    return @result;
}

sub store {
    my $self = shift;
    my $cond = $self->map_name_to_rec($_[0]);
    my $record = $_[1];
    my $crecord = $self->get_record($cond);
    if ($crecord) {
        $self->update_record($cond, $record);
    } else {
        $self->insert_record($record);
    }
}

sub drop {
    my $self = shift;
    my $record = $self->map_name_to_rec($_[0]);
    my $sql = "DELETE FROM $self->{table} WHERE ";
    $sql .= join(' AND ', map { "$_ = ?" } keys %$record);
    return $self->{'dbh'}->do($sql, undef, values %$record);
}

sub map_name_to_rec {
    my $self = shift;
    my $name = shift;
    my @keys = $self->get_primary_key();
    my @values = split /[$self->{fn_sep}]/, $name, scalar @keys;
    return undef unless $#values == $#keys;
    my $i = 0;
    my %result;
    %result = map { $_ => $values[$i++] } @keys;
    return \%result;
}

# @param hashref condition
# @param bool full
# @return list|hashref record
sub get_record {
    my $self = shift;
    my ($condition, $full) = @_;
    my @keys = keys %$condition;
    my $sql = "SELECT ". ($full? "*": join(',', @keys));
    $sql .= " FROM $self->{table} WHERE ". join(' AND ', map { "$_ = ?" } @keys);
    return wantarray? $self->{'dbh'}->selectrow_array($sql, undef, values %$condition): $self->{'dbh'}->selectrow_hashref($sql, undef, values %$condition);
}

sub insert_record {
    my $self = shift;
    my $record = shift;
    my $sql = "INSERT INTO $self->{table} (";
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

1;
# }}}

# Query operations {{{
package FusqlFS::Base::Queries;
use strict;
our @ISA = ('FusqlFS::Base::Module');

sub execute_queries {
    my $self = shift;
    my $buffer = shift;
    foreach (split /;\n/, $$buffer) {
        s/^\s+//; s/\s+$//;
        next unless $_;
        $self->{'dbh'}->do($_);
    }
}

sub get {
    my $self = shift;
    my $query = shift;

    my $sth = $self->{'dbh'}->prepare($query);
    return -1 unless $sth->execute();

    my $buffer = $sth->fetchall_arrayref({});
    $sth->finish();
    return $buffer;
}

1;
# }}}

# Roles operations {{{
package FusqlFS::Base::Roles;
use strict;
our @ISA = ('FusqlFS::Base::Module');

1;
# }}}

# Views operations {{{
package FusqlFS::Base::Views;
use strict;
our @ISA = ('FusqlFS::Base::Module');

1;
# }}}
