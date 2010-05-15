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
    'maxcached'     => 0,
);

GetOptions(
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
    'maxcached|M:i'   => \$options{'maxcached'},
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

=head2 Basic options

=item B<--host, -h>

    Host name to connect, defaults to localhost.

=item B<--port, -P>

    Port number to connect to, default depends on database engine in use.

=item B<--user, -u>

    Username to authorize.

=item B<--password, -p>

    Password to authorize.

=item B<--database, --db, -d>

    Database name to connect to. Mandatory.

=item B<--mountpoint, -m>

    Mointpoint, must be an empty directory. Mandatory.

=item B<--engine, -e>

    DB engine to use. Can be either PgSQL or MySQL for now. PgSQL is really
    implemented, MySQL is in my todo list. Defaults to PgSQL.

=head2 Other options with values

=item B<--charset, -C>

    Default charset, used for tables creation, results display etc.
    Defaults to current locale's charset.

=item B<--fnsep, -s>

    File name fields separator, used to compose filenames out from multi-field
    primary keys. If you have table with primary key like (obj_id, name), every
    record in DB will be visible as a file with its name composed of this two
    fields (like "12.odrie", "43.nanny" etc.) This option's value is used as a
    separator to glue field values togather. Defaults to single dot (.).
    You may wish to change it if you have table with text fields in primary key
    with a dot in them.

=item B<--limit, -L>

    Integer, number of data rows to show in table's data subdir, defaults to 0
    (means all rows). Useful if you are going to browse really big databases,
    as listing all data records as files can be very slow and memory consuming.

    You can also limit number of cache entries with --maxcached options (see
    below). HDD-backed cache can be implemented in the future.
    
    If this is an issue for you, use this option to limit number of listed
    table rows. You can still get record by requesting filename equal to
    primary key value (id usually) directly, if you know it, even if you don't
    see it in directory listing.

=item B<--maxcached, -M>

    Integer, number of max cache items to store. If number of cached items
    exceeds this number, cache cleanup is forced, least used entries removed
    first. Values below or equal to zero means unlimited cache entries.
    Defaults to 0, i.e. unlimited cache.

    I recommend to set this value to at least 3/4 of total objects in your
    database (including all tables, sequences, views, data rows and other
    objects, browsable with this program), which is about 60% cache hits (~45%
    for 1/2 and ~56% for 2/3). But this is just a basic recomendation based on
    educated guess and some tests with "entry" names generated with normally
    distributed random generator. Experiment is your best advisor in this case.

    Also set this value above zero only if you really have low memory issues
    with the program, as this cache method have to support additional
    structures to analyze data usage, and so it's slower than simple unlimited
    cache strategy (it's still much faster than database requests, however).

=head2 Boolean options

=item B<--innodb>

    Boolean, MySQL specific. If set, new tables created by the program use
    InnoDB backend, MyISAM is used otherwise. Defaults to false (MyISAM).

=item B<--debug, -D>

    Boolean, if set you will see more debug info.

=item B<--daemon>

    Boolean, if set the program will daemonize itself. Defaults to true. You
    may wish to use it as --nodeamon to debug the program.

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
