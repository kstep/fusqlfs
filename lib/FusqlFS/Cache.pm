use strict;
use v5.10.0;

package FusqlFS::Cache;
use Carp;

use FusqlFS::Cache::Limited;
use FusqlFS::Cache::File;

sub init
{
    my $class = shift;
    my $hash = shift;
    my $strategy = shift;
    my $subclass = '';

    given ($strategy)
    {
        when ('memory')  { }
        when ('limited') { $subclass = '::Limited' }
        when ('file')    { $subclass = '::File' }
        default
        {
            carp "Unknown cache strategy `$strategy', using default `memory' strategy";
        }
    }

    return unless $subclass;

    $class .= $subclass;
    unless ($class->is_needed(@_))
    {
        carp "Given parameters don't match `$strategy' cache strategy, falling back to `memory' strategy";
        return;
    }

    tie %$hash, $class, @_;
}

1;
