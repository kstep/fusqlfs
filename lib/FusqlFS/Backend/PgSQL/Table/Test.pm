use strict;
use v5.10.0;

package FusqlFS::Backend::PgSQL::Table::Test;
use FusqlFS::Backend::PgSQL::Test;

our $fusqlh;

sub set_up
{
    $fusqlh = FusqlFS::Backend::PgSQL::Test->set_up();
    return unless $fusqlh;
    $fusqlh->{subpackages}->{tables}->create('fusqlfs_table');
}

sub tear_down
{
    FusqlFS::Backend::PgSQL::Test->tear_down();
}

1;
