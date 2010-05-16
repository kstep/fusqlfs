use strict;
use v5.10.0;

package FusqlFS::Base::Entry;

sub new
{
    my ($class, $fs, $path, $leaf_absent) = @_;

    $path =~ s{^/}{};
    $path =~ s{/$}{};
    my @path = split /\//, $path;

    my $entry = $fs->{subpackages};
    my $pkg = $entry;
    my @names = ();
    my @tail = ();
    foreach my $p (@path)
    {
        return unless defined $entry;
        if (UNIVERSAL::isa($entry, 'FusqlFS::Base::Interface'))
        {
            @tail = ();
            $pkg = $entry;
            $entry = $pkg->get(@names, $p);
            push @names, $p;
        }
        elsif (my $ref = ref $entry)
        {
            given ($ref)
            {
                when ('HASH')  { $entry = $entry->{$p} || undef }
                when ('ARRAY') { $entry = $entry->[$p] || undef }
                default        { undef $entry }
            }
            push @tail, $p;
        }
    }

    $entry ||= $leaf_absent;
    return unless defined $entry;
    my $list;
    if (UNIVERSAL::isa($entry, 'FusqlFS::Base::Interface'))
    {
        $pkg = $entry;
        $list = $pkg->list(@names);
        $entry = $pkg->get(@names) unless $list;
    }
    elsif (my $ref = ref $entry)
    {
        given ($ref)
        {
            when ('HASH')  { $list = [ keys %$entry ] }
            when ('ARRAY') { $list = [ 0..$#{$entry} ] }
            #when ('SCALAR') {}
        }
    }
    my $self = [ $pkg, \@names, $entry, $list, \@tail ];
    bless $self, $class;
}

sub get { $_[0]->[2] }
sub list { $_[0]->[3] }
sub move
{
    my $self = shift;
    my $target = shift;

    unless ($self->depth())
    {
        $self->pkg()->rename($self->names(), $target->name());
    }
    else
    {
        my $entry = $target->tailref();
        $entry = $self->tailref($entry, undef);
        $self->pkg()->store($self->names(), $entry);
    }
}
sub drop { $_[0]->put(undef) or $_[0]->[0]->drop(@{$_[0]->[1]}); }
sub create { $_[0]->put('') or $_[0]->[0]->create(@{$_[0]->[1]}); }
sub store { my $data = $_[1]||$_[0]->[2]; $_[0]->put($data) or $_[0]->[0]->store(@{$_[0]->[1]}, $data); }

sub put
{
    my $self = shift;
    my $data = shift;

    unless ($self->depth())
    {
        return;
    }
    else
    {
        my $entry = $self->tailref(undef, $data);
        $self->pkg()->store($self->names(), $entry);
    }
    return 1;
}

sub tailref
{
    my $self = shift;
    my @tail = $self->tail();
    my $tail = pop @tail;
    my $entry = shift || $self->entry();
    my $data = @_? shift: $self->get();
    my $tailref = $entry;
    $tailref = ref $tailref eq 'HASH'? $tailref->{$_}: $tailref->[$_] foreach (@tail);
    given (ref $tailref)
    {
        when ('HASH')  { if (defined $data) { $tailref->{$tail} = $data } else { delete $tailref->{$tail} } }
        when ('ARRAY') { if (defined $data) { $tailref->[$tail] = $data } else { delete $tailref->[$tail] } }
    }
    return $entry;
}

sub isdir { defined $_[0]->[3] }
sub islink { ref $_[0]->[2] eq 'SCALAR' }
sub isfile { !(defined $_[0]->[3] || ref $_[0]->[2]) }
sub isdirty { defined $_[0]->[5] }

sub writable { !UNIVERSAL::isa($_[0]->[2], 'FusqlFS::Base::Interface') }

sub pkg { $_[0]->[0] }
sub names { @{$_[0]->[1]} }
sub tail { @{$_[0]->[4]} }
sub name { $_[0]->[4]->[-1] || $_[0]->[1]->[-1] }
sub depth { scalar @{$_[0]->[4]} }
sub height { scalar @{$_[0]->[1]} }

sub entry { $_[0]->[0]->get(@{$_[0]->[1]}) }
sub write { $_[0]->[5] = 1; substr($_[0]->[2], $_[1], length($_[2]||$_[0]->[2])) = $_[2]||''; }
sub flush { return unless defined $_[0]->[5]; $_[0]->store($_[0]->[2]); pop @{$_[0]}; }

1;

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

sub load
{
    return $FusqlFS::Base::instance->{loader}->($_[1]);
}

sub dump
{
    return $FusqlFS::Base::instance->{dumper}->($_[1]) if $_[1];
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
use FusqlFS::Cache;

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
        cache  => {},
        dbh => DBI->connect($dsn, @options{qw(user password)}),
    };
    bless $self, $class;

    if ($options{maxcached} && $options{maxcached} > 0)
    {
        $self->{cache} = new FusqlFS::Cache::Limited($options{maxcached});
    }
    $SIG{'USR1'} = sub{ %{$self->{cache}} = (); };

    $instance = $self;
    $self->init();
    return $self;
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

sub by_path
{
    my ($self, $path) = @_;
    $self->{cache}->{$path} = new FusqlFS::Base::Entry(@_) unless defined $self->{cache}->{$path};
    return $self->{cache}->{$path};
}

sub by_path_uncached
{
    new FusqlFS::Base::Entry(@_);
}

sub clear_cache
{
    my $self = shift;
    delete $self->{cache}->{$_[0]};
    if (defined $_[1])
    {
        my $key = $_[0];
        $key =~ s{/[^/]+$}{} for (0..$_[1]);
        foreach (keys %{$self->{cache}})
        {
            next unless /^$key/;
            delete $self->{cache}->{$_};
        }
    }
}

sub destroy
{
    undef $instance;
}

1;

