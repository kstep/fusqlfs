use strict;
use v5.10.0;

package FusqlFS::Backend;

=head1 NAME

FusqlFS::Backend - FusqlFS database backend abstract factory

=head1 SYNOPSIS

    use FusqlFS::Backend;

    my $fs = FusqlFS::Backend->new(engine => 'PgSQL', database => 'dbname', user => 'postgres', password => 'pa$$w0rd');

=head1 DESCRIPTION

This class is a factory for the family of L<FusqlFS::Backend::Base> subclasses.
Its C<new()> method initializes and returns some kind of real backend class
instance (L<FusqlFS::Backend::PgSQL> or L<FusqlFS::Backend::MySQL> for now)
depending on `engine' option passed to it. If engine is not given or not
recognized, it falls back to `PgSQL' backend with a debug message.

You better look at one of underlying classes for detailed description.

=cut

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
