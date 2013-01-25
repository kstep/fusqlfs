use strict;
use 5.010;

package FusqlFS::Formatter::Htmltable;
use parent 'FusqlFS::Formatter::Html';
use FusqlFS::Version;
our $VERSION = $FusqlFS::Version::VERSION;

=head1 NAME

FusqlFS::Formatter::Htmltable - HTML formatter class

=head1 DESCRIPTION

This formatter outputs data as HTML tables.

This is a dump-only formatter, it doesn't parse data back into Perl structures,
so you can't edit and save data while using this formatter.

See also L<FusqlFS::Formatter::Html> formatter for data formatted in HTML lists.

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
            my $index = 0;
            return '<table class="table"><thead><tr><th>#</th><th>Value</th></tr></thead><tbody><tr>' . join('</tr><tr>', map "<th>${\(++$index)}</th><td>${\_Dump($_)}</td>", @$value) . '</tr></table>';
        }
        when ('HASH') {
            return '<table class="table"><tbody><tr>' . join('</tr><tr>', map "<th>$_</th><td>${\_Dump($value->{$_})}</td>", sort keys %$value) . '</tr></tbody></table>';
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
