use strict;
use v5.10.0;

package FusqlFS::Cache;
use Carp;

use FusqlFS::Cache::Limited;
use FusqlFS::Cache::File;

=begin testing init

#!noinst

foreach (qw(Limited File))
{
    my %cache;
    ok !FusqlFS::Cache->init(\%cache, lc $_, 0), $_.' cache strategy not chosen';
    ok !tied(%cache), $_.' cache handler is untied';

    isa_ok FusqlFS::Cache->init(\%cache, lc $_, 10), 'FusqlFS::Cache::'.$_, $_.' cache strategy chosen';
    isa_ok tied(%cache), 'FusqlFS::Cache::'.$_, $_.' cache handler tied';
}

my %cache;
ok !FusqlFS::Cache->init(\%cache, 'memory'), 'Memory cache strategy chosen';
ok !FusqlFS::Cache->init(\%cache, 'xxxxxx'), 'Memory cache strategy chosen (fallback 1)';
ok !FusqlFS::Cache->init(\%cache), 'Memory cache strategy chosen (fallback 2)';
ok !tied(%cache), 'Memory cache handler is untied';

=end testing
=cut
sub init
{
    my $class = shift;
    my $hash = shift;
    my $strategy = shift || 'memory';
    my $subclass = '';

    given ($strategy)
    {
        when ('memory')  { }
        when ('limited') { $subclass = '::Limited' }
        when ('file')    { $subclass = '::File' }
        default
        {
            #carp "Unknown cache strategy `$strategy', using default `memory' strategy";
        }
    }

    return unless $subclass;

    $class .= $subclass;
    unless ($class->is_needed(@_))
    {
        #carp "Given parameters don't match `$strategy' cache strategy, falling back to `memory' strategy";
        return;
    }

    tie %$hash, $class, @_;
}

1;
