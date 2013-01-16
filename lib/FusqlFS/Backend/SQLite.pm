use strict;
use 5.010;

package FusqlFS::Backend::SQLite;
use FusqlFS::Version;
our $VERSION = $FusqlFS::Version::VERSION;
use parent 'FusqlFS::Artifact';

use FusqlFS::Backend::SQLite::Tables;
use FusqlFS::Backend::SQLite::Indices;

sub init
{
    $_[0]->{subpackages} = {
        tables  => new FusqlFS::Backend::SQLite::Tables(),
        indices => new FusqlFS::Backend::SQLite::Indices(),
    };
}

sub dsn
{
    say STDERR "SQLite:dbname=$_[3]";
    return "SQLite:dbname=$_[3]";
}

1;
