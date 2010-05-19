use strict;
use v5.10.0;

package FusqlFS::Entry;

sub new
{
    my ($class, $fs, $path, $leaf_absent) = @_;

    my $subclass = '::File';
    $path =~ s{^/}{};
    $path =~ s{/$}{};
    my @path = split /\//, $path;

    my $entry = $fs->{subpackages};
    my $pkg = $entry;
    my @names = ();
    my @tail = ();
    foreach my $p (@path)
    {
        return unless defined $entry;
        if (UNIVERSAL::isa($entry, 'FusqlFS::Base::Interface'))
        {
            @tail = ();
            $pkg = $entry;
            $entry = $pkg->get(@names, $p);
            push @names, $p;
        }
        elsif (my $ref = ref $entry)
        {
            given ($ref)
            {
                when ('HASH')  { $entry = defined $entry->{$p}? $entry->{$p}: undef }
                when ('ARRAY') { $entry = defined $entry->[$p]? $entry->[$p]: undef }
                default        { undef $entry }
            }
            push @tail, $p;
        }
    }

    $entry = $leaf_absent unless defined $entry;
    return unless defined $entry;
    my $list;
    if (UNIVERSAL::isa($entry, 'FusqlFS::Base::Interface'))
    {
        $pkg = $entry;
        $list = $pkg->list(@names);
        if ($list) {
            $subclass = '::Dir';
        } else {
            $entry = $pkg->get(@names);
            if (my $ref = ref $entry)
            {
                given ($ref)
                {
                    when ('SCALAR') { $subclass = '::Symlink' }
                    when ('CODE')   { $subclass = '::Pipe' }
                }
            }
        }
    }
    elsif (my $ref = ref $entry)
    {
        given ($ref)
        {
            when ('HASH')   { $subclass = '::Dir' }
            when ('ARRAY')  { $subclass = '::Dir' }
            when ('SCALAR') { $subclass = '::Symlink' }
            when ('CODE')   { $subclass = '::Pipe' }
        }
    }
    my $self = [ $pkg, \@names, $entry, $list, \@tail, undef ];
    bless $self, $class.$subclass;
    $self->init();
    return $self;
}

sub init { }
sub get { $_[0]->[2] }
sub size { length $_[0]->[2] }
sub list { }
sub move
{
    my $self = shift;
    my $target = shift;

    unless ($self->depth())
    {
        $self->pkg()->rename($self->names(), $target->name());
    }
    else
    {
        my $entry = $target->tailref();
        $entry = $self->tailref($entry, undef);
        $self->pkg()->store($self->names(), $entry);
    }
}
sub drop { $_[0]->put(undef) or $_[0]->[0]->drop(@{$_[0]->[1]}); }
sub create { $_[0]->put('') or $_[0]->[0]->create(@{$_[0]->[1]}); }
sub store { my $data = $_[1]||$_[0]->[2]; $_[0]->put($data) or $_[0]->[0]->store(@{$_[0]->[1]}, $data); return 1; }

sub put
{
    my $self = shift;
    my $data = shift;

    unless ($self->depth())
    {
        return;
    }
    else
    {
        my $entry = $self->tailref(undef, $data);
        $self->pkg()->store($self->names(), $entry);
    }
    return 1;
}

sub tailref
{
    my $self = shift;
    my @tail = $self->tail();
    my $tail = pop @tail;
    my $entry = shift || $self->entry();
    my $data = @_? shift: $self->get();
    my $tailref = $entry;
    $tailref = ref $tailref eq 'HASH'? $tailref->{$_}: $tailref->[$_] foreach (@tail);
    given (ref $tailref)
    {
        when ('HASH')  { if (defined $data) { $tailref->{$tail} = $data } else { delete $tailref->{$tail} } }
        when ('ARRAY') { if (defined $data) { $tailref->[$tail] = $data } else { delete $tailref->[$tail] } }
    }
    return $entry;
}

sub isdir { }
sub islink { }
sub isfile { }
sub ispipe { }
sub isdirty { defined $_[0]->[5] }

sub writable { !UNIVERSAL::isa($_[0]->[2], 'FusqlFS::Base::Interface') }

sub pkg { $_[0]->[0] }
sub names { @{$_[0]->[1]} }
sub tail { @{$_[0]->[4]} }
sub name { $_[0]->[4]->[-1] || $_[0]->[1]->[-1] }
sub depth { scalar @{$_[0]->[4]} }
sub height { scalar @{$_[0]->[1]} }

sub entry { $_[0]->[0]->get(@{$_[0]->[1]}) }
sub write { $_[0]->[5] = ''; substr($_[0]->[2], $_[1], length($_[2]||$_[0]->[2])) = $_[2]||''; }
sub flush { return unless defined $_[0]->[5]; $_[0]->store($_[0]->[2]) and pop @{$_[0]}; }

1;

package FusqlFS::Entry::File;
use base 'FusqlFS::Entry';

sub isfile { return 1; }

1;

package FusqlFS::Entry::Pipe;
use base 'FusqlFS::Entry';

sub init
{
    # 0=pkg, 1=names, 2=filter sub, 3=input buffer, 4=tail, 5=output buffer
    undef $_[0]->[3];
    $_[0]->[5] = $_[0]->[2]->();
}

sub ispipe { return 1; }

sub size { length $_[0]->[5] }
sub get { my $buffer = $_[0]->[5]; $_[0]->[5] = $_[0]->[2]->(); return $buffer; }
sub write { $_[0]->[3] ||= ''; substr($_[0]->[3], $_[1], length($_[2]||$_[0]->[3])) = $_[2]||''; }
sub flush { return unless defined $_[0]->[3]; $_[0]->[5] = $_[0]->[2]->($_[0]->[3]); undef $_[0]->[3]; }

1;

package FusqlFS::Entry::Dir;
use base 'FusqlFS::Entry';

sub init
{
    # 0=pkg, 1=names, 2=dir entry, 3=list buffer, 4=tail
    return if defined $_[0]->[3];
    $_[0]->[3] = ref $_[0]->[2] eq 'HASH'? [ keys %{$_[0]->[2]} ]: [ 0..$#{$_[0]->[2]} ];
}

sub size { scalar @{$_[0]->[3]} }
sub isdir { return 1; }
sub list { $_[0]->[3] }

1;

package FusqlFS::Entry::Symlink;
use base 'FusqlFS::Entry';

sub size { length ${$_[0]->[2]} }
sub islink { return 1; }

1;

