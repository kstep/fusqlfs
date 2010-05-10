use v5.10.0;
use strict;

package Base::Entry;

sub new
{
    my ($class, $fs, $path, $leaf_absent) = @_;

    $path =~ s{^/}{};
    $path =~ s{/$}{};
    my @path = split /\//, $path;

    my $entry = $fs;
    my $pkg = $entry;
    my @names = ();
    foreach my $p (@path)
    {
        return unless UNIVERSAL::isa($entry, 'HASH');
        if (UNIVERSAL::isa($entry, 'Base::Interface'))
        {
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
            $entry = $entry->{$p} || undef;
        }
    }

    $entry ||= $leaf_absent;
    return unless defined $entry;
    my $list;
    if (UNIVERSAL::isa($entry, 'Base::Interface'))
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
            when ('ARRAY') { $list = $entry }
            #when ('SCALAR') {}
        }
    }
    my $self = [ $pkg, \@names, $entry, $list ];
    bless $self, $class;
}

sub get { $_[0]->[2] }
sub list { $_[0]->[3] }
sub rename { $_[0]->[0]->rename(@{$_[0]->[1]}, $_[1]) }
sub drop { $_[0]->[0]->drop(@{$_[0]->[1]}) }
sub create { $_[0]->[0]->create(@{$_[0]->[1]}) }
sub store { $_[0]->[0]->store(@{$_[0]->[1]}, $_[1]) }

sub isdir { defined $_[0]->[3] }
sub islink { ref $_[0]->[2] eq 'SCALAR' }
sub isduty { defined $_[0]->[4] }

sub write { $_[0]->[4] = 1; substr($_[0]->[2], $_[1], length($_[2]||$_[0]->[2])) = $_[2]||''; }
sub flush { $_[0]->store($_[0]->[2]); delete $_[0]->[4]; }

1;

package Base::Interface;

sub new { bless {}, $_[0] }
sub get { return '' }
sub list { return }
sub rename { return 1 }
sub drop { return 1 }
sub create { return 1 }
sub store { return 1 }

1;

package Base::Root;
use base 'Base::Interface';

use DBI;
use YAML::Tiny;

our $dbh;
our $dumper;
our $loader;

our %cache;

sub new
{
    my $class = shift;
    my $self = {};
    bless $self, $class;

    my $dsn = 'DBI:'.$self->dsn(@_[0..2]);
    $dbh = DBI->connect($dsn, @_[-2,-1]);
    $dumper = \&YAML::Tiny::Dump;
    $loader = \&YAML::Tiny::Load;

    $self->init();
    return $self;
}

sub dsn
{
    my $dsn = "";
    $dsn .= ";host=$_[1]" if $_[1];
    $dsn .= ";port=$_[2]" if $_[2];
    $dsn .= ";database=$_[3]";
    return $dsn;
}

sub init
{
    return;
}

sub by_path
{
    $cache{$_[1]} = new Base::Entry(@_) unless defined $cache{$_[1]};
    return $cache{$_[1]};
}

sub clear_cache
{
    delete $cache{$_[1]};
}

1;
