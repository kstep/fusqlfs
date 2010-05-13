#!/usr/bin/perl

use strict;

use Getopt::Long;
use Pod::Usage;
use Carp;

use POSIX qw(locale_h setsid);
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
    'engine'        => 'PgSQL',
    'innodb'        => 0,
    'limit'         => 0,
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
    'limit|L:i'       => \$options{'limit'},
) or pod2usage(2);

$options{'database'} ||= $ARGV[0];
$options{'mountpoint'} ||= $ARGV[1];

pod2usage(1) unless !$options{'help'} && $options{'database'} && $options{'mountpoint'};

#if ($use_threads) {
#	use threads;
#	use threads::shared;
#}

daemonize($options{'logfile'}) if $options{'daemon'};

FusqlFS::init(%options);

FusqlFS::mount( $options{'mountpoint'}, 
    'mountopts'  => $options{'allow_other'}? 'allow_other': '',
    'debug'      => $options{'debug'},
    'threaded'   => $use_threads,
);

sub daemonize {
    my $logfile = shift;

    if ($logfile) {
        open STDERR, ">>", $logfile;
        select((select(\*STDERR), $| = 1)[0]);
    }

    my $ppid = $$;
    my $pid = fork and exit 0;
    croak('Can\'t daemonize') unless defined $pid;
    select undef, undef, undef, .001 while (kill 0, $ppid);
    my $sid = setsid();
    die() if $sid == -1;
    chdir '/';
    umask 00;
    close STDIN or croak('Unable to close STDIN');
    close STDOUT or croak('Unable to close STDOUT');
    unless ($logfile) {
        close STDERR or croak('Unable to close STDERR');
    }
    return $sid;
}

__END__

=head1 NAME

    fusqlfs - FUSE file system to mount DB and provide tools to control and admin it

=head1 SYNOPSIS

    fusqlfs [options] database directory

=head1 EXAMPLES

    fusqlfs dbname ./mnt
    fusqlfs --host=localhost --port=5432 --engine=PgSQL --user=postgres --password=12345 dbname ./mnt
    fusqlfs --database=dbname --user=postgres --mountpoint=./mnt
    fusqlfs -d dbname -m ./mnt -u postgres -p 12345 -e PgSQL

=head1 OPTIONS

=over 8

=item B<--host, -h>

    Host name to connect, defaults to localhost.

=item B<--port, -P>

    Port number to connect to, default depends on database engine in use.

=item B<--user, -u>

    Username to authorize.

=item B<--password, -p>

    Password to authorize.

=item B<--charset, -C>

    Default charset, used for tables creation, results display etc.
    Defaults to current locale's charset.

=item B<--mountpoint, -m>

    Mointpoint, must be an empty directory. Mandatory.

=item B<--database, -d>

    Database name to connect to. Mandatory.

=item B<--innodb>

    Boolean, MySQL specific. If set, new tables created by the program use
    InnoDB backend, MyISAM is used otherwise. Defaults to false (MyISAM).

=item B<--fnsep, -s>

    File name fields separator, used to compose filenames out from multi-field
    primary keys. If you have table with primary key like (obj_id, name), every
    record in DB will be visible as a file with its name composed of this two
    fields (like "12.odrie", "43.nanny" etc.) This option's value is used as a
    separator to glue field values togather. Defaults to single dot (.).
    You may wish to change it if you have table with text fields in primary key
    with a dot in them.

=item B<--debug, -D>

    Boolean, if set you will see more debug info.

=item B<--engine, -e>

    DB engine to use. Can be either PgSQL or MySQL for now. PgSQL is really
    implemented, MySQL is in my todo list.

=item B<--daemon>

    Boolean, if set the program will daemonize itself. Defaults to true. You
    may wish to use it as --nodeamon to debug the program.

=item B<--limit, -L>

    Integer, number of data rows to show in table's data subdir, defaults to 0
    (means all rows). Useful if you are going to browse really big databases,
    as listing all data records as files can be very slow and memory consuming.

    All data are buffered in memory for now, which is OK for small DBs (the
    most usual case for developers as they work with almost empty development
    database), but for big tables this approach be a show stopper, so I'm going
    to add some kind of adaptive caching (cache only small subset of data we
    are working with now and drop unused cache entries on memory low
    condition), or HDD-backed caching, or both.
    
    If this is an issue for you, use this option to limit number of loaded
    table rows. You can still get record by requesting filename equal to
    primary key value (id usually) directly, if you know it, even if you don't
    see it in directory listing.
    

=back

=head1 DESCRIPTION

This FUSE-daemon allows to mount any DB as a simple filesystem. Unlike other
similar "sqlfs" filesystem, it doesn't provide simple DB-backed file storage,
but given you full interface to all database internals.

Every table, view, function etc. is a directory, every single field, index,
record etc. is a file, symlink or subdirectory in the mounted filesystem. You
can create table "mkdir ./mnt/tables/tablename", and remove them with "rmdir"
afterwards. You can edit fields as simple YAML-files. All your usual file
utilities are at your service including "find", "rm", "ln", "mv", "cp" etc.

Just mount your DB and enjoy!

=head1 TODO

    * Implement MySQL support.
    * Implement PgSQL views, functions, sequences etc.
    * Write better docs: describe FS structure, rules and precautions to use it
    as DB admin tool.
