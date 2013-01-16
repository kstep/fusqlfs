use strict;
use 5.010;

package FusqlFS::Backend::MySQL;
use FusqlFS::Version;
our $VERSION = $FusqlFS::Version::VERSION;
use parent 'FusqlFS::Backend::Base';

use FusqlFS::Backend::MySQL::Tables;
use FusqlFS::Backend::MySQL::Users;
use FusqlFS::Backend::MySQL::Procedures;
use FusqlFS::Backend::MySQL::Functions;
use FusqlFS::Backend::MySQL::Variables;

sub init
{
    my $self = shift;
    $self->do('SET character_set_results = ?'   , $self->{charset});
    $self->do('SET character_set_client = ?'    , $self->{charset});
    $self->do('SET character_set_connection = ?', $self->{charset});

    $self->{subpackages} = {
        tables => new FusqlFS::Backend::MySQL::Tables(),
        users  => new FusqlFS::Backend::MySQL::Users(),

        procedures => new FusqlFS::Backend::MySQL::Procedures(),
        functions  => new FusqlFS::Backend::MySQL::Functions(),

        variables => new FusqlFS::Backend::MySQL::Variables(),
    };
}

sub dsn
{
    my $self = shift;
    return 'mysql:'.$self->SUPER::dsn(@_);
}

1;

