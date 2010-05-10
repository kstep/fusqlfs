use v5.10.0;
use strict;

package Base::Entry;

sub new
{
    my $class = shift;
    my $fs = shift;
    my $path = shift;

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
            $entry = $entry->{$p};
        }
    }

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
sub rename { $_[0]->[0]->rename(@{$_[0]->[1]}, @_) }
sub drop { $_[0]->[0]->drop(@{$_[0]->[1]}) }
sub create { $_[0]->[0]->create(@{$_[0]->[1]}) }
sub store { $_[0]->[0]->store(@{$_[0]->[1]}, @_) }

sub isdir { defined $_[0]->[3] }
sub islink { ref $_[0]->[2] eq 'SCALAR' }

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

sub new
{
    shift;
    my $class = shift;
    $dbh = DBI->connect(@_);
    $dumper = \&YAML::Tiny::Dump;

    my $self = {
        cache => {},
    };

    bless $self, $class;
}

sub by_path
{
    $_[0]->{cache}->{$_[1]} = new Base::Entry($_[0], $_[1]) unless defined $_[0]->{cache}->{$_[1]};
    return $_[0]->{cache}->{$_[1]};
}

sub clear_cache
{
    delete $_[0]->{cache}->{$_[1]};
}

1;
