use strict;
use v5.10.0;

package FusqlFS::Artifact;

our $instance;

=begin testing Artifact

#!noinst

isa_ok FusqlFS::Artifact->new(), 'FusqlFS::Artifact';
is FusqlFS::Artifact->get(), '';
is FusqlFS::Artifact->list(), undef;
foreach my $method (qw(rename drop create store))
{
    is FusqlFS::Artifact->$method(), 1;
}

=end testing
=cut
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
    return $instance->{dbh}->prepare($sql);
}

sub cexpr
{
    my ($self, $sql, @sprintf) = @_;
    $sql = sprintf($sql, @sprintf) if @sprintf;
    return $instance->{dbh}->prepare_cached($sql, {}, 1);
}

sub do
{
    my ($self, $sql, @binds) = @_;
    $sql = sprintf($sql, @{shift @binds}) if !ref($sql) && ref($binds[0]);
    $instance->{dbh}->do($sql, {}, @binds);
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
    return $instance->{dbh}->selectrow_hashref($sql, {}, @binds);
}

sub all_col
{
    my ($self, $sql, @binds) = @_;
    $sql = sprintf($sql, @{shift @binds}) if !ref($sql) && ref($binds[0]);
    return $instance->{dbh}->selectcol_arrayref($sql, {}, @binds);
}

sub all_row
{
    my ($self, $sql, @binds) = @_;
    $sql = sprintf($sql, @{shift @binds}) if !ref($sql) && ref($binds[0]);
    return $instance->{dbh}->selectall_arrayref($sql, { Slice => {} }, @binds);
}

sub load
{
    return $_[1] if ref $_[1];
    return $instance->{loader}->($_[1]);
}

sub dump
{
    return $instance->{dumper}->($_[1]) if $_[1];
    return;
}

sub limit
{
    my $limit = $instance->{limit};
    return $limit? "LIMIT $limit": '';
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
        push @binds, [ @bind ] if @bind;
    }
    $sql = $instance->{dbh}->prepare($sql);
    $sql->bind_param($_+1, @{$binds[$_]}) foreach (0..$#binds);
    return $sql;
}

1;

