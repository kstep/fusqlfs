#!/usr/bin/perl
use v5.10.0;
use strict;

use FsDescr;
use Data::Dump qw(dump);

our $fs = new FsDescr('DBI:Pg:database=unite_dev', 'unite_dev', 'GtIXQeugXO4I');
say $fs;

sub map_path_to_obj
{
    return $fs->by_path($_[0]);
}

my $entry = map_path_to_obj('/tables');
say dump $entry->[3];

__END__

my $entry = map_path_to_obj('/');
say dump $entry;

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
my $t = map_path_to_obj('/tables/testtablecreate');
$t->create();
$ind = $t->list();
say dump $ind;
$t->drop();
$ind = $t->list();
say dump $ind;

my $ind = map_path_to_obj('/tables/profiles/data');
say dump $ind;
my $entry = map_path_to_obj('/tables/profiles/data/22223');
say $entry;
