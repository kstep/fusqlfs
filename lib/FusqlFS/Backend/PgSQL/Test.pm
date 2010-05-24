use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Test;
use base 'Exporter';

use Test::More;
our @EXPORT = qw(list_ok);

our $fusqlh;

sub dbi_connect
{
    use DBI;
    return DBI->connect('DBI:Pg:database=postgres', 'postgres', '', { PrintError => 0, PrintWarn => 0 });
}

sub set_up
{
    my $dbh = dbi_connect();
    $dbh->do("DROP DATABASE IF EXISTS fusqlfs_test");
    $dbh->do("CREATE DATABASE fusqlfs_test");
    $dbh->disconnect;

    use FusqlFS::Backend::PgSQL;
    $fusqlh = FusqlFS::Backend::PgSQL->new(host => '', port => '', database => 'fusqlfs_test', user => 'postgres', password => '');
}

sub tear_down
{
    $fusqlh->destroy();

    my $dbh = dbi_connect();
    $dbh->do("DROP DATABASE IF EXISTS fusqlfs_test");
    $dbh->disconnect;
}

sub list_ok
{
    my ($list, $expected, $name) = @_;
    ok defined $list, $name;
    is ref($list), 'ARRAY', $name;
    if (ref($expected) eq 'CODE')
    {
        ok $expected->(@$list), $name;
    }
    else
    {
        is_deeply $list, $expected, $name;
    }
}

1;
