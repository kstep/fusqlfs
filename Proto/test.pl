#!/usr/bin/perl
use FsDescr;
use Data::Dump qw(dump);
use strict;
use feature ':5.10';

my $fs = new FsDescr('DBI:Pg:database=unite_dev', 'unite_dev', 'GtIXQeugXO4I');

my $ind = $fs->{subpackages}->{tables}->{subpackages}->{indices}->list('profiles');
say dump $ind;

my $ind = $fs->{subpackages}->{tables}->{subpackages}->{indices}->get('profiles', 'ddddd');
say dump $ind;

my $ind = $fs->{subpackages}->{tables}->{subpackages}->{struct}->list('profiles');
say dump $ind;

my $ind = $fs->{subpackages}->{tables}->{subpackages}->{struct}->get('profiles', 'id');
say $ind;

my $t = $fs->{subpackages}->{tables};
$t->create('testtablecreate');
$ind = $t->list();
say dump $ind;
$t->drop('testtablecreate');
$ind = $t->list();
say dump $ind;

my $ind = $t->{subpackages}->{data}->list('profiles');
say dump $ind;
$ind = $t->{subpackages}->{data}->get('profiles', '22223');
say $ind;
