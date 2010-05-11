use v5.10.0;
use strict;
use FusqlFS::Base;

package MySQL::Base;
use base 'FusqlFS::Base';

sub new
{
    my $self = new FusqlFS::Base::Root(@_);
    return $self;
}

1;

