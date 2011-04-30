use strict;
use 5.010;

package FusqlFS::Backend::MySQL::Table::Test;
our $VERSION = "0.005";
use FusqlFS::Backend::MySQL::Test;

our $fusqlh;

sub set_up
{
    $fusqlh = FusqlFS::Backend::MySQL::Test->set_up();
    return unless $fusqlh;
    $fusqlh->{subpackages}->{tables}->create('fusqlfs_table');
}

sub tear_down
{
    FusqlFS::Backend::MySQL::Test->tear_down();
}

1;
