use strict;
use 5.010;

package FusqlFS::Backend::SQLite::Tables;
use FusqlFS::Version;
our $VERSION = $FusqlFS::Version::VERSION;
use parent 'FusqlFS::Artifact';

#sub init
#{
    #my $self = shift;
    #say STDERR defined $self->dbh()? 1: 0;
    #$self->{type} = 'table';
    #$self->SUPER::init(@_);
#}

1;
