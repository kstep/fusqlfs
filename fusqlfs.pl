#!/usr/bin/perl

use strict;

use Getopt::Long;
use POSIX qw(locale_h setsid);
#require "$dirname/FusqlFS.pm";
use FusqlFS;

my $use_threads = 0;
my $locale = setlocale(LC_CTYPE);
if ($locale =~ /\.([-A-Za-z0-9]+)$/) {
    $locale = lc $1;
    $locale =~ s/-//g;
} else {
    $locale = '';
}

my %options = (
    'host'          => 'localhost',
    'port'          => '',
    'database'      => '',
    'user'          => 'root',
    'password'      => '',
    'mountpoint'    => '',
    'debug'         => 0,
    'help'          => 0,
    'charset'       => $locale,
    'daemon'        => 1,
    'logfile'       => '',
    'fnsep'         => '.',
    'engine'        => 'MySQL',
    'innodb'        => 0,
);

GetOptions(
    'o:s'             => \%options,
    'host|h:s'        => \$options{'host'},
    'port|P:i'        => \$options{'port'},
    'database|db|d:s' => \$options{'database'},
    'user|u:s'        => \$options{'user'},
    'password|p:s'    => \$options{'password'},
    'mountpoint|m:s'  => \$options{'mountpoint'},
    'debug|D'         => \$options{'debug'},
    'help'            => \$options{'help'},
    'charset|C:s'     => \$options{'charset'},
    'daemon!'         => \$options{'daemon'},
    'logfile|l:s'     => \$options{'logfile'},
    'fnsep|s:s'       => \$options{'fnsep'},
    'engine|e:s'      => \$options{'engine'},
    'innodb!'         => \$options{'innodb'},
) or show_help();

$options{'database'} ||= $ARGV[0];
$options{'mountpoint'} ||= $ARGV[1];

show_help() unless !$options{'help'} && $options{'database'} && $options{'mountpoint'};

#if ($use_threads) {
#	use threads;
#	use threads::shared;
#}

daemonize($options{'logfile'}) if $options{'daemon'};

FusqlFS::initialize(%options);

FusqlFS::main(
    'mountpoint' => $options{'mountpoint'},
    'mountopts'  => $options{'allow_other'}? 'allow_other': '',
    'debug'      => $options{'debug'},
    'threaded'   => $use_threads,
);

sub daemonize {
    my $logfile = shift;

    if ($logfile) {
        open \*STDERR, ">>", $logfile;
        select((select(\*STDERR), $| = 1)[0]);
    }

    my $ppid = $$;
    my $pid = fork and exit 0;
    die "Can't daemonize!\n" unless defined $pid;
    select undef, undef, undef, .001 while (kill 0, $ppid);
    my $sid = setsid();
    die() if $sid == -1;
    chdir '/';
    umask 00;
    close \*STDIN or die();
    close \*STDOUT or die();
    unless ($logfile) {
        close STDERR or die();
    }
    return $sid;
}

sub show_help {
    my ($myname) = ($0 =~ m{/([^/]+)$});
    print "Usage:\n";
    print "\t$myname [-o <name>=<value> [...]]\n";
    print "\t\t\{--database=<database>|-d <database>|<database>\}\n";
    print "\t\t\{--mountpoint=<mountpoint>|-m <mountpoint>|<mountpoint>\}\n";
    print "\t\t[--host=<hostname>|-h <hostname>] [--port=<port>|-P <port>]\n";
    print "\t\t[--user=<username>|-u <username>] [--password=<password>|-p <password>]\n";
    print "\t\t[--charset=<charset>|-C <charset>] [--[no]innodb] [--fnsep=<sep>|-s <sep>]\n";
    print "\t\t[--debug|-D] [--[no]daemon] [--logfile=<logfile>|-l <logfile>]\n";
    print "\t\t[--engine={MySQL|PgSQL}|-e {MySQL|PgSQL}";
    print "\t$myname --help\n\n";
    print <<HELP;

All names are selfexplaining. All parameters can be passed with
-o option, e.g. -o host=localhost is identical to --host=localhost.
Mountpoint and database are required parameters, they can be passed
as a separate strings or using --database and --mountpoint parameters
(or -o database=... -o mountpoint=...).

If --daemon is set (the default option), daemon will be really
daemonized. Use --nodaemon option to make daemon run in foreground.
If run in daemon mode, you can set log file with --logfile option.

If --innodb is set, daemon will try to use InnoDB engine for table
creation. Works with MySQL engine only (see below).

If --fnsep is set, it will be used as a separator in
filenames to divide e.g. values in record-files, fieldname and
subpart in indexed fields-symlinks etc. It is extremly useful
for databases, where fields from primary indexes can contain the default
for this parameter (single dot).

There're two types of databases supported: MySQL and PostgreSQL.
They are served by two different modules called "engines": MySQL and PgSQL.
You can set engine to use with --engine options, which defaults to MySQL.
PostgreSQL support is under development and experimental for now.

HELP
    exit;
}
