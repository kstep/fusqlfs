use strict;
use v5.10.0;

package FusqlFS::Artifact::Table::Lazy;
use parent 'FusqlFS::Artifact';

sub new
{
    my $class = shift;
    my $self = {};
    
    $self->{create_cache} = {};
    $self->{template} = {};
    
    bless $self, $class;
}

=begin testing clone

is_deeply {_tpkg}::clone({ a => 1, b => 2, c => 3 }), { a => 1, b => 2, c => 3 };
is_deeply {_tpkg}::clone([ 3, 2, 1 ]), [ 3, 2, 1 ];
is_deeply {_tpkg}::clone(\'string'), \'string';
is_deeply {_tpkg}::clone({ a => [ 3, 2, 1 ], b => { c => 1, d => [ 6, \5, 4 ] }, c => \"string" }),
    { a => [ 3, 2, 1 ], b => { c => 1, d => [ 6, \5, 4 ] }, c => \"string" };

=end testing
=cut
sub clone
{
    my $ref = $_[0];
    my $result;
    given (ref $ref)
    {
        when ('HASH')   { $result = { map { $_ => clone($ref->{$_}) } keys %$ref } }
        when ('ARRAY')  { $result = [ map { clone($_) } @$ref ] }
        when ('SCALAR') { my $tmp = $$ref; $result = \$tmp; }
        default         { $result = $ref; }
    }
    return $result;
}

=begin testing create after get list

isnt $_tobj->create('table', 'name'), undef;
isnt $_tobj->get('table', 'name'), $_tobj->{template};
is_deeply $_tobj->get('table', 'name'), $_tobj->{template};
is_deeply $_tobj->list('table'), [ 'name' ];

=end testing
=cut
sub create
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->{create_cache}->{$table} ||= {};
    $self->{create_cache}->{$table}->{$name} = clone($self->{template});
    return 1;
}

=begin testing drop after rename

isnt $_tobj->drop('table', 'newname'), undef;
is $_tobj->get('table', 'newname'), undef;
is_deeply $_tobj->list('table'), [];

=end testing
=cut
sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    if (exists $self->{create_cache}->{$table}->{$name})
    {
        delete $self->{create_cache}->{$table}->{$name};
        return 1;
    }
    return;
}

=begin testing rename after create

is $_tobj->rename('table', 'aname', 'anewname'), undef;
is $_tobj->get('table', 'aname'), undef;
is $_tobj->get('table', 'anewname'), undef;

isnt $_tobj->rename('table', 'name', 'newname'), undef;
is $_tobj->get('table', 'name'), undef;
is_deeply $_tobj->get('table', 'newname'), $_tobj->{template};
is_deeply $_tobj->list('table'), [ 'newname' ];

=end testing
=cut
sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    if (exists $self->{create_cache}->{$table}->{$name})
    {
        $self->{create_cache}->{$table}->{$newname} = $self->{create_cache}->{$table}->{$name};
        delete $self->{create_cache}->{$table}->{$name};
        return 1;
    }
    return;
}

=begin testing list

is_deeply $_tobj->list('table'), [], 'list is sane';

=end testing
=cut
sub list
{
    my $self = shift;
    my ($table) = @_;
    return [ keys %{$self->{create_cache}->{$table}||{}} ];
}

=begin testing get

is $_tobj->get('table', 'name'), undef, 'get is sane';
is $_tobj->get('table', 'name'), undef, 'get has no side effects';

=end testing
=cut
sub get
{
    my $self = shift;
    my ($table, $name) = @_;
    return $self->{create_cache}->{$table}->{$name}||undef;
}

1;

