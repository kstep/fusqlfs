use strict;
use 5.010;

package FusqlFS::Backend::MySQL::Users;
our $VERSION = "0.005";
use parent 'FusqlFS::Artifact';

sub init
{
    my $self = shift;
    $self->{list_expr} = $self->expr("SELECT CONCAT(User, '\@', Host) FROM mysql.user");
    $self->{get_expr} = $self->expr("SELECT * FROM mysql.user WHERE User = ? AND Host = ?");
    $self->{create_expr} = $self->expr("CREATE USER '%(User)s'\@'%(Host)s'");
    $self->{drop_expr} = $self->expr("DROP USER '%(User)s'\@'%(Host)s'");
}

sub list
{
    my $self = shift;
    return $self->all_col($self->{list_expr})||[];
}

sub get
{
    my $self = shift;
    my ($user, $host) = split(/@/, shift, 2);
    return unless $user || $host;

    my $data = $self->one_row($self->{get_expr}, $user, $host);
    return unless $data;

    my @privileges = ();
    my %result = ();
    while (my ($name, $value) = each %$data) {
        if ($name =~ /_priv$/) {
            $name =~ s/_priv$//;
            push @privileges, $name if $value == 'Y';
        } else {
            $result{$name} = $value;
        }
    }
    $result{privileges} = \@privileges;

    return $self->dump(\%result);
}

sub create
{
    my $self = shift;
    my ($user, $host) = split(/@/, shift, 2);
    return unless $user || $host;

    $self->do($self->{create_expr}, {User => $user, Host => $host});
}

sub drop
{
    my $self = shift;
    my ($user, $host) = split(/@/, shift, 2);
    return unless $user || $host;

    $self->do($self->{drop_expr}, {User => $user, Host => $host});
}

__END__

