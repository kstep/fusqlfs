use strict;
use 5.010;

package FusqlFS::Backend::MySQL::Artifact;
our $VERSION = "0.005";
use parent 'FusqlFS::Artifact';

=begin testing concat

is $_tobj->concat("one", "two", "three"), "CONCAT_WS('.', `one`, `two`, `three`)";

=end testing

=cut
sub concat
{
    shift @_;
    my $instance = $FusqlFS::Artifact::instance;
    return "CONCAT_WS('$instance->{fnsep}', `" . join('`, `', @_) . "`)";
}

1;
