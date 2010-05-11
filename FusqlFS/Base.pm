use v5.10.0;
use strict;

package FusqlFS::Base::Entry;

sub new
{
    my ($class, $fs, $path, $leaf_absent) = @_;

    $path =~ s{^/}{};
    $path =~ s{/$}{};
    my @path = split /\//, $path;

    my $entry = $fs;
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
            if ($entry)
            {
                push @names, $p;
            }
            else
            {
                $entry = $pkg->{subpackages}->{$p} || undef;
            }
        }
        else
        {
            given (ref $entry)
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
        unless ($list)
        {
            $list = [ keys %{$pkg->{subpackages}} ] if exists $pkg->{subpackages};
        }
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
sub rename { $_[0]->[0]->rename(@{$_[0]->[1]}, $_[1]) }
sub drop { $_[0]->[0]->drop(@{$_[0]->[1]}) }
sub create { $_[0]->[0]->create(@{$_[0]->[1]}) }
sub store
{
    my $self = shift;
    my $data = shift || $self->get();
    my @tail = $self->tail();
    unless (@tail)
    {
        $self->pkg()->store($self->names(), $data);
    }
    else
    {
        my $entry = $self->entry();
        my $tail = pop @tail;
        my $tailref = $entry;
        $tailref = ref $tailref eq 'HASH'? $tailref->{$_}: $tailref->[$_] foreach (@tail);
        given (ref $tailref)
        {
            when ('HASH')  { $tailref->{$tail} = $data }
            when ('ARRAY') { $tailref->[$tail] = $data }
        }
        $self->pkg()->store($self->names(), $entry);
    }
}

sub isdir { defined $_[0]->[3] }
sub islink { ref $_[0]->[2] eq 'SCALAR' }
sub isdirty { defined $_[0]->[5] }

sub pkg { $_[0]->[0] }
sub names { @{$_[0]->[1]} }
sub tail { @{$_[0]->[4]} }

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

1;

package FusqlFS::Base;
use base 'FusqlFS::Base::Interface';

use DBI;
use YAML::Tiny;

our $dbh;
our $dumper;
our $loader;
our $instance;

our %cache;

sub new
{
    return $instance if $instance;

    my $class = shift;
    my $self = {};
    bless $self, $class;

    my $dsn = 'DBI:'.$self->dsn(@_[0..2]);
    $dbh = DBI->connect($dsn, @_[-2,-1]);
    $dumper = \&YAML::Tiny::Dump;
    $loader = \&YAML::Tiny::Load;

    $self->init();
    $instance = $self;
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
    $cache{$_[1]} = new FusqlFS::Base::Entry(@_) unless defined $cache{$_[1]};
    return $cache{$_[1]};
}

sub clear_cache
{
    delete $cache{$_[1]};
    if ($_[2])
    {
        foreach (keys %cache)
        {
            next unless /^$_[1]/;
            delete $cache{$_};
        }
    }
}

sub destroy
{
    undef $instance;
}

sub DESTROY
{
    $dbh->disconnect();
    undef $dbh;
    undef $dumper;
    undef $loader;
    undef %cache;
}
1;

