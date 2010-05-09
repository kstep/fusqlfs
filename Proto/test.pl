#!/usr/bin/perl
use FsDescr;
use Data::Dump qw(dump);
use strict;
use feature ':5.10';

our $fs = new FsDescr('DBI:Pg:database=unite_dev', 'unite_dev', 'GtIXQeugXO4I');

sub map_path_to_obj
{
    my $path = shift;
    my $pkg = $fs;
    my $pkglvl = 0;
    my $entry = $pkg->{subpackages};
    my @names = ();

    $path =~ s{^/}{};
    $path =~ s{/$}{};
    my @path = split /\//, $path;
    foreach my $p (@path)
    {
        if (exists $entry->{$p})
        {
            $entry = $entry->{$p};
            if (UNIVERSAL::isa($entry, 'Interface'))
            {
                $pkglvl++;
                $pkg = $entry;
                $entry = $entry->{subpackages} if exists $entry->{subpackages};
            }
        }
        else
        {
            push @names, $p;
        }
    }
    if ($pkg == $entry)
    {
        $entry = $pkglvl == scalar(@names)? $entry->get(@names): $entry->list(@names);
    }
    return wantarray? ($entry, $pkg, @names): $entry;
}

# /tables/profiles/indices
my $entry = map_path_to_obj('/tables/profiles/indices');
say dump $entry;

# /tables/profiles/indices/ddddd
$entry = map_path_to_obj('/tables/profiles/indices/ddddd');
say dump $entry;

# /tables/profiles/struct
my $ind = map_path_to_obj('/tables/profiles/struct');
say dump $ind;

# /tables/profiles/struct/id
my $ind = map_path_to_obj('/tables/profiles/struct/id');
say $ind;

# /tables
my ($_, $t) = map_path_to_obj('/tables');
$t->create('testtablecreate');
$ind = $t->list();
say dump $ind;
$t->drop('testtablecreate');
$ind = $t->list();
say dump $ind;

my $ind = map_path_to_obj('/tables/profiles/data');
say dump $ind;
my $entry = map_path_to_obj('/tables/profiles/data/22223');
say $entry;
