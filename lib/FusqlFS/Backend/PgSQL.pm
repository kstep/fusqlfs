use strict;
use 5.010;

package FusqlFS::Backend::PgSQL;
our $VERSION = "0.005";
use parent 'FusqlFS::Backend::Base';

use FusqlFS::Backend::PgSQL::Tables;
use FusqlFS::Backend::PgSQL::Views;
use FusqlFS::Backend::PgSQL::Sequences;
use FusqlFS::Backend::PgSQL::Roles;
use FusqlFS::Backend::PgSQL::Queries;
use FusqlFS::Backend::PgSQL::Functions;
use FusqlFS::Backend::PgSQL::Languages;

=begin testing

#!class FusqlFS::Backend::PgSQL::Test
#!noinst

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
        functions => new FusqlFS::Backend::PgSQL::Functions(),
        languages => new FusqlFS::Backend::PgSQL::Languages(),
    };
}

sub dsn
{
    my $self = shift;
    return 'Pg:'.$self->SUPER::dsn(@_);
}

1;

