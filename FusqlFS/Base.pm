use strict;
use v5.10.0;

package FusqlFS::Base::Interface;

sub new { bless {}, $_[0] }
sub get { return '' }
sub list { return }
sub rename { return 1 }
sub drop { return 1 }
sub create { return 1 }
sub store { return 1 }

sub expr
{
    my ($self, $sql, @sprintf) = @_;
    $sql = sprintf($sql, @sprintf) if @sprintf;
    return $FusqlFS::Base::instance->{dbh}->prepare($sql);
}

sub cexpr
{
    my ($self, $sql, @sprintf) = @_;
    $sql = sprintf($sql, @sprintf) if @sprintf;
    return $FusqlFS::Base::instance->{dbh}->prepare_cached($sql, {}, 1);
}

sub do
{
    my ($self, $sql, @binds) = @_;
    $sql = sprintf($sql, @{shift @binds}) if !ref($sql) && ref($binds[0]);
    $FusqlFS::Base::instance->{dbh}->do($sql, {}, @binds);
}

sub cdo
{
    my ($self, $sql, @binds) = @_;
    $sql = $self->cexpr($sql, !ref($sql) && ref($binds[0])? @{shift @binds}: undef);
    return $sql if $sql->execute(@binds);
}

sub one_row
{
    my ($self, $sql, @binds) = @_;
    $sql = sprintf($sql, @{shift @binds}) if !ref($sql) && ref($binds[0]);
    return $FusqlFS::Base::instance->{dbh}->selectrow_hashref($sql, {}, @binds);
}

sub all_col
{
    my ($self, $sql, @binds) = @_;
    $sql = sprintf($sql, @{shift @binds}) if !ref($sql) && ref($binds[0]);
    return $FusqlFS::Base::instance->{dbh}->selectcol_arrayref($sql, {}, @binds);
}

sub all_row
{
    my ($self, $sql, @binds) = @_;
    $sql = sprintf($sql, @{shift @binds}) if !ref($sql) && ref($binds[0]);
    return $FusqlFS::Base::instance->{dbh}->selectall_arrayref($sql, { Slice => {} }, @binds);
}

sub load
{
    return $FusqlFS::Base::instance->{loader}->($_[1]);
}

sub dump
{
    return $FusqlFS::Base::instance->{dumper}->($_[1]) if $_[1];
    return;
}

sub limit
{
    my $limit = $FusqlFS::Base::instance->{limit};
    return "LIMIT $limit" if $limit;
}

sub build
{
    my ($self, $sql, $filter, @iter) = @_;
    my (@binds, @bind);
    foreach (@iter)
    {
        local $_ = $_;
        next unless (@bind) = ($filter->());
        $sql .= shift @bind;
        push @binds, [ @bind ];
    }
    $sql = $FusqlFS::Base::instance->{dbh}->prepare($sql);
    $sql->bind_param($_+1, @{$binds[$_]}) foreach (0..$#binds);
    return $sql;
}

1;

package FusqlFS::Base;
use base 'FusqlFS::Base::Interface';

use DBI;
use YAML::Tiny;
use FusqlFS::Entry;

our $instance;

sub new
{
    return $instance if $instance;

    my $class = shift;
    my %options = @_;
    my $dsn = 'DBI:'.$class->dsn(@options{qw(host port database)});
    my $self = {
        subpackages => {},
        dumper => \&YAML::Tiny::Dump,
        loader => \&YAML::Tiny::Load,
        limit  => 0 + $options{limit},
        dbh => DBI->connect($dsn, @options{qw(user password)}),
    };
    bless $self, $class;

    $instance = $self;
    $self->init();
    return $self;
}

sub by_path
{
    return FusqlFS::Entry->new(@_);
}

sub dsn
{
    my $dsn = "";
    $dsn .= "host=$_[1];" if $_[1];
    $dsn .= "port=$_[2];" if $_[2];
    $dsn .= "database=$_[3];";
    return $dsn;
}

sub init
{
    return;
}


sub destroy
{
    undef $instance;
}

1;

