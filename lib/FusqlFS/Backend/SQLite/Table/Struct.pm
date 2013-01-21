use strict;
use 5.010;

package FusqlFS::Backend::SQLite::Table::Struct;
use FusqlFS::Version;
our $VERSION = $FusqlFS::Version::VERSION;
use parent 'FusqlFS::Artifact';

sub init
{
    my $self = shift;

    $self->{list_expr} = "PRAGMA table_info(%s)";
    $self->{tables_cache} = {};
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    my $list = $self->all_row($self->{list_expr}, [$table]);
    return unless $list && @$list;

    my %table_info = map { $_->{name}, {
        nullable => !$_->{notnull},
        type     => $_->{type},
        default  => $_->{dflt_value},
        order    => $_->{cid},
        pk       => !!$_->{pk},
    } } @$list;
    $self->{tables_cache}->{$table} = \%table_info;

    return [ keys %table_info ];
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    my $result = $self->{tables_cache}->{$table} || undef;

    unless ($result) {
        $self->list($table);
        $result = $self->{tables_cache}->{$table} || undef;
    }

    return unless $result && $result->{$name};
    return $self->dump($result->{$name});
}

1;

__END__
