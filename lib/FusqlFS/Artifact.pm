use strict;
use v5.10.0;

package FusqlFS::Artifact;

=head1 NAME

FusqlFS::Artifact - basic abstract class to represent database artifact in FusqlFS

=head1 SYNOPSIS

    package FusqlFS::Backend::PgSQL::Tables;
    use parent 'FusqlFS::Artifact';

    sub new
    {
        my $class = shift;
        my $self = {};

        // initialize Tables specific resources

        bless $self, $class;
    }

    sub get
    {
        my $self = shift;
        my ($table, $name) = @_;
        return $self->one_row("SELECT * FROM %s WHERE id = ?", [$table], $name);
    }

    sub list
    {
        my $self = shift;
        my ($table) = @_;
        return $self->all_col("SELECT id FROM %s %s", [$table, $self->limit]);
    }

=head1 DESCRIPTION

This abstract class declares interface between database artifacts (like tables,
data rows, functions, roles etc.) and L<Fuse> hooks in L<FusqlFS>.

The point of this class is to abstract database layer interaction from file
system structure operations, so it provides some basic operations under
database artifacts like "get", "list", "create", "drop", etc.

For example L<FusqlFS::Backend::PgSQL::Tables> subclass defines it's
L<get|FusqlFS::Backend::PgSQL::Tables/get> method to return table's description
and L<list|FusqlFS::Backend::PgSQL::Tables/list> method to list all available
tables, so this subclass is represented as directory with tables in file system.

For more examples see childrens of this class.

=head1 METHODS

=over

=cut

our $instance;

=item Basic interface methods

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

=over

=item new

Fallback constructor, shouldn't be called at all.

Input: $class
Output: $artifact_instance.

=item get

Get item from this artifact.

Input: @names.
Output: $hashref|$arrayref|$scalarref|$coderef|$scalar|undef.

Hashrefs and arrayref are represented as directories in filesystem with keys
(or indices in case of arrayref) as filenames and values as their content
(maybe hashrefs or arrayrefs as well).

Scalarrefs are represented as symlinks, their content being the path to
referenced object in filesystem.

Coderefs provide "pseudopipes" interface: at first request referenced sub is
called without parameters for initialization and file content will be whatever
the sub returns. On any write to the "pseudopipe" the sub is called with
written data as first argument and the content of the file will be any text the
sub returns back. Dynamic DB queries in L<FusqlFS::Backend::PgSQL::Queries>
class are implemented with this interface.

Scalars are represented with plain files.

If this sub returns undef the file with given name is considered non-existant,
and user will get C<NOENT> error.

=item list

Get list of items, represented by class.

Input: @names.
Output: $arrayref|undef.

If this method returns arrayref of scalars, then the class is represented with
directory containing elements with names from this array, otherwise (the method
returns undef) the type of filesystem object is determined solely on L</get>
method call results.

=item rename

Renames given database artifact.

Input: @names, $newname.
Output: $success.

This method must rename database object defined with @names to new $newname
and return any "true" value on success or undef on failure.

=item drop

Removes given database artifact.

Input: @names.
Output: $success.

This method must drop given database object defined with @names and return
any "true" value on success or undef on failure.

=item create

Creates brand new database artifact.

Input: @names.
Output: $success.

This method must create new database object by given @name and return any
"true" value on success or undef on failure. If given object can't be created
without additional "content" data (e.g. table's index) it should create some
kind of stub in memory/cache/anywhere and this stub must be visible via L</get>
and L</list> methods giving the user a chance to fill it with some real data,
so successive L</store> call can create the object.

=item store

Stores any changes to object in database.

Input: @names, $data.
Output: $success.

This method must accept the same $data structure as provided by L</get> method,
possibly modified by user, and store it into database, maybe creating actual
database object in process (see L</create> for details).
The method must return any "true" value on success or undef on failure.

=back

=cut
sub new { bless {}, $_[0] }
sub get { return '' }
sub list { return }
sub rename { return 1 }
sub drop { return 1 }
sub create { return 1 }
sub store { return 1 }

sub dbh
{
    $instance->{dbh};
}

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

sub fnsep
{
    return $instance->{fnsep};
}

sub asplit
{
    return split $instance->{fnsplit}, $_[1];
}

sub ajoin
{
    shift @_;
    return join $instance->{fnsep}, @_;
}

sub concat
{
    shift @_;
    return '"' . join("\" || '$instance->{fnsep}' || \"", @_) . '"';
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

__END__

=back
