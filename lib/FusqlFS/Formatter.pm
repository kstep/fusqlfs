use strict;
use 5.010;

package FusqlFS::Formatter;
use FusqlFS::Version;
our $VERSION = $FusqlFS::Version::VERSION;

=head1 NAME

FusqlFS::Formatter - formatter backend for FusqlFS

=head1 SYNOPSIS

    my ($dump, $load) = FusqlFS::Formatter->init('yaml');
    print $dump->($data);
    my $data = $load->($data);

=head1 DESCRIPTION

This class is a frontend to different data formatters for FusqlFS.

=head1 METHODS

=over

=cut

our %FORMATTERS = (
    xml => [
        'XML/Simple.pm',
        sub { XMLout($_[0], NoAttr => 1) },
        sub { XMLin($_[0], NoAttr => 1) },
        ],
    yaml => [
        'YAML/Tiny.pm',
        \&YAML::Tiny::Dump,
        \&YAML::Tiny::Load,
        ],
    json => [
        'JSON/Syck.pm',
        \&JSON::Syck::Dump,
        \&JSON::Syck::Load,
        ],
);

=item init

Initialize dumper and loader for given output format.

Input: $format
Output: &dumper, &loader

=begin testing init

#!noinst
my ($dump, $load) = FusqlFS::Formatter->init('native');
is ref $dump, 'CODE', 'Dumper defined';
is ref $load, 'CODE', 'Loader defined';

=end testing
=cut
sub init
{
    my $class = shift;
    my $format = shift;

    my $formatter;
    if (exists $FORMATTERS{$format}) {
        $formatter = $FORMATTERS{$format};
        require $formatter->[0] if $formatter->[0];
        return $formatter->[1], $formatter->[2];
    } else {
        my $package = ucfirst lc $format;
        require "FusqlFS/Formatter/$package.pm";
        return eval qq{\\&FusqlFS::Formatter::${package}::Dump}, eval qq{\\&FusqlFS::Formatter::${package}::Load};
    }
}

1;

__END__

=back
