use strict;
use 5.010;

package FusqlFS::Backend::SQLite;
use FusqlFS::Version;
our $VERSION = $FusqlFS::Version::VERSION;
use parent 'FusqlFS::Backend::Base';

use FusqlFS::Backend::SQLite::Tables;

sub init
{
    $_[0]->{subpackages} = {
        tables  => new FusqlFS::Backend::SQLite::Tables(),
    };
}

sub dsn
{
    say STDERR "SQLite:dbname=$_[3]";
    return "SQLite:dbname=$_[3]";
}

1;
