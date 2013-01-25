use strict;
use 5.010;

package FusqlFS::Formatter::Html;
use parent 'FusqlFS::Formatter::Base';
use FusqlFS::Version;
our $VERSION = $FusqlFS::Version::VERSION;

=head1 NAME

FusqlFS::Formatter::Html - HTML formatter class

=head1 DESCRIPTION

This formatter outputs data as lists (ordered C<E<lt>olE<gt>> lists for arrays
and definition C<E<lt>dlE<gt>> lists for hashes).

This is a dump-only formatter, it doesn't parse data back into Perl structures,
so you can't edit and save data while using this formatter.

See also L<FusqlFS::Formatter::Htmltable> formatter for data formatted in HTML
tables.

=cut

sub Dump
{
    '<html><head><meta charset="utf-8" /><title>FusqlFS data</title><link href="http://netdna.bootstrapcdn.com/twitter-bootstrap/2.2.2/css/bootstrap.no-icons.min.css" rel="stylesheet"></head><body>' . &_Dump . '</body></html>';
}

sub _Dump
{
    my $value = shift;
    my $ref = ref $value;
    return $value unless $ref;

    given ($ref) {
        when ('ARRAY') {
            return '<ol><li>' . join('</li><li>', map _Dump($_), @$value) . '</li></ol>';
        }
        when ('HASH') {
            return '<dl>' . join('', map "<dt>$_</dt><dd>${\_Dump($value->{$_})}</dd>", sort keys %$value) . '</dl>';
        }
        when ('SCALAR') {
            return $$value;
        }
        default {
            return $value;
        }
    }
}

1;
