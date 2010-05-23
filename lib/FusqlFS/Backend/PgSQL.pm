use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL;
use parent 'FusqlFS::Backend::Base';

use FusqlFS::Backend::PgSQL::Tables;
use FusqlFS::Backend::PgSQL::Views;
use FusqlFS::Backend::PgSQL::Sequences;
use FusqlFS::Backend::PgSQL::Roles;
use FusqlFS::Backend::PgSQL::Queries;

=begin testing

# Testing environment preparation
use DBI;
my $dbh = DBI->connect('DBI:Pg:database=postgres', 'postgres', '');
BAIL_OUT 'Unable to connect PostgreSQL: '.$DBI::errstr unless $dbh;

$dbh->do('DROP DATABASE IF EXISTS fusqlfs_test');
$dbh->do('CREATE DATABASE fusqlfs_test') or BAIL_OUT 'Unable to create test database: '.$dbh->errstr;
$dbh->disconnect;

# Initialize backend
require_ok 'FusqlFS::Backend::PgSQL';
my $fusqlh = FusqlFS::Backend::PgSQL->new(
    host     => '',
    port     => '',
    database => 'fusqlfs_test',
    user     => 'postgres',
    password => ''
);

isa_ok $fusqlh, 'FusqlFS::Backend::PgSQL', 'PgSQL backend initialization';

my $new_fusqlh = FusqlFS::Backend::PgSQL->new();
is $new_fusqlh, $fusqlh, 'PgSQL backend is singleton';

=end testing
=cut

sub init
{
    $_[0]->{subpackages} = {
        tables    => new FusqlFS::Backend::PgSQL::Tables(),
        views     => new FusqlFS::Backend::PgSQL::Views(),
        sequences => new FusqlFS::Backend::PgSQL::Sequences(),
        roles     => new FusqlFS::Backend::PgSQL::Roles(),
        queries   => new FusqlFS::Backend::PgSQL::Queries(),
    };
}

sub dsn
{
    my $self = shift;
    return 'Pg:'.$self->SUPER::dsn(@_);
}

1;

