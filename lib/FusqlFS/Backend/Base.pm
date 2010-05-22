use strict;
use v5.10.0;

use FusqlFS::Interface;

package FusqlFS::Backend::Base;
use base 'FusqlFS::Interface';

use DBI;
use FusqlFS::Entry;

sub new
{
    return $FusqlFS::Interface::instance if $FusqlFS::Interface::instance;

    my $class = shift;
    my %options = @_;
    my $dsn = 'DBI:'.$class->dsn(@options{qw(host port database)});
    my $self = {
        subpackages => {},
        limit  => 0 + $options{limit},
        dbh => DBI->connect($dsn, @options{qw(user password)}),
    };

    given ($options{format})
    {
        when ('xml')
        {
            use XML::Simple;
            $self->{dumper} = sub () { XMLout($_[0], NoAttr => 1) };
            $self->{loader} = sub () { XMLin($_[0], NoAttr => 1) };
        }
        when ('yaml')
        {
            use YAML::Tiny;
            $self->{dumper} = \&YAML::Tiny::Dump;
            $self->{loader} = \&YAML::Tiny::Load;
        }
        when ('json')
        {
            use JSON::Syck;
            $self->{dumper} = \&JSON::Syck::Dump;
            $self->{loader} = \&JSON::Syck::Load;
        }
        default
        {
            use YAML::Tiny;
            $self->{dumper} = \&YAML::Tiny::Dump;
            $self->{loader} = \&YAML::Tiny::Load;
        }
    }

    bless $self, $class;

    $FusqlFS::Interface::instance = $self;
    $self->init();
    return $self;
}

sub by_path
{
    return FusqlFS::Entry->new(@_);
}

sub dsn
{
    my $dsn = "";
    $dsn .= "host=$_[1];" if $_[1];
    $dsn .= "port=$_[2];" if $_[2];
    $dsn .= "database=$_[3];";
    return $dsn;
}

sub init
{
    return;
}


sub destroy
{
    undef $FusqlFS::Interface::instance;
}

1;

