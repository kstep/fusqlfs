use v5.10.0;
use strict;
use Base;

package MySQL::Root;
use base 'Base::Root';

sub new
{
    my $self = new Base::Root(@_);
    return $self;
}

1;

