use strict;
use 5.010;

package FusqlFS::Backend::MySQL;
use FusqlFS::Version;
our $VERSION = $FusqlFS::Version::VERSION;
use parent 'FusqlFS::Backend::Base';

sub init
{
    my $self = shift;
    $self->do('SET character_set_results = ?'   , $self->{charset});
    $self->do('SET character_set_client = ?'    , $self->{charset});
    $self->do('SET character_set_connection = ?', $self->{charset});

    $self->autopackages(
        'tables',
        'users',
        'procedures',
        'functions',
        'variables');
}

sub dsn
{
    my $self = shift;
    return 'mysql:'.$self->SUPER::dsn(@_);
}

1;

