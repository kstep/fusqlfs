#!/usr/bin/perl

use strict;
use v5.10.0;

use Module::Build;

my $class = Module::Build->subclass(
    class => 'FusqlFS::Builder',
    code  => q{
        eval 'use Module::Build::Debian';

        sub ACTION_buildtests
        {
            use Test::Inline;
            my $tests = Test::Inline->new(
                verbose     => 1,
                readonly    => 0,
                check_count => 2,
                input       => 'lib',
                output      => 't',
                manifest    => 't/manifest',
            );
            $tests->add_all;
            $tests->save;
        }
    },
);

my $build = $class->new(
    module_name   => 'FusqlFS',
    license       => 'gpl3',
    dist_author   => 'Konstantin Stepanov <kstep@p-nut.info>',
    dist_abstract => 'fusqlfs - FUSE file system to mount DB and provide tools to control and admin it',
    requires => {
        'perl'         => '5.010',

        'Getopt::Long' => 0,
        'Pod::Usage'   => 0,
        'Carp'         => 0,

        'DBI'          => '1.600',
        'DBD::Pg'      => 0,

        'YAML::Tiny'   => 0,

        'POSIX'        => 0,
        'Fcntl'        => 0,
        'Fuse'         => '0.09',
    },
    build_requires => {
        'Test::More'   => 0,
        'Test::Inline' => 0,
    },
    recommends => {
        'DBD::mysql'   => 0,

        'XML::Simple'  => 0,
        'JSON::Syck'   => 0,
    },
    add_to_cleanup => [ '*.bak', 'FusqlFS-*' ],
);

$build->create_build_script;

