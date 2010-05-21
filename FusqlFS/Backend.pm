use strict;
use v5.10.0;

package FusqlFS::Backend;
use Carp;

use FusqlFS::Backend::PgSQL;
use FusqlFS::Backend::MySQL;

sub new
{
    my $class = shift;
    my %options = @_;
    my $engine = $options{engine};
    my $subclass = '';

    given (lc $engine)
    {
        when ('pgsql') { $subclass = '::PgSQL' }
        when ('mysql') { $subclass = '::MySQL' }
        default
        {
            carp "Unknown engine `$engine', falling back to default `PgSQL' engine";
            $subclass = '::PgSQL';
        }
    }

    $class .= $subclass;
    $class->new(@_);
}

1;
