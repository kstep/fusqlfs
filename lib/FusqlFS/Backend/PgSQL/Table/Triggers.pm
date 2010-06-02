use strict;
use v5.10.0;

=head1 NAME

FusqlFS::Backend::PgSQL::Table::Triggers

=head1 SYNOPSIS

    use FusqlFS::Backend::PgSQL::Table::Triggers

    my $triggers = FusqlFS::Backend::PgSQL::Table::Triggers->new();

=head1 DESCRIPTION

=head1 EXPOSED STRUCTURE

=over

=back

=head1 METHODS

=over

=cut

package FusqlFS::Backend::PgSQL::Table::Triggers;
use parent 'FusqlFS::Artifact::Table::Lazy';

sub new
{
    my $class = shift;
    my $self = {};

    # tgtype:
    # 0  1 = for each row
    # 1  2 = before
    # 2  4 = insert
    # 3  8 = delete
    # 4 16 = update
    # 5 32 = truncate (?)
    $self->{get_expr} = $class->expr('SELECT pg_catalog.pg_get_triggerdef(t.oid) AS "create.sql",
            p.proname||\'(\'||pg_catalog.pg_get_function_arguments(p.oid)||\')\' AS handler,
            CASE WHEN (t.tgtype & 1) != 0 THEN \'row\' ELSE \'statement\' END AS for_each,
            CASE WHEN (t.tgtype & 2) != 0 THEN \'before\' ELSE \'after\' END AS when,
            ARRAY[
                CASE WHEN (t.tgtype &  4) != 0 THEN \'insert\' END,
                CASE WHEN (t.tgtype &  8) != 0 THEN \'delete\' END,
                CASE WHEN (t.tgtype & 16) != 0 THEN \'update\' END
                CASE WHEN (t.tgtype & 32) != 0 THEN \'truncate\' END
                ] AS events
        FROM pg_catalog.pg_trigger AS t
            LEFT JOIN pg_catalog.pg_class AS r ON (t.tgrelid = r.oid)
            LEFT JOIN pg_catalog.pg_proc AS p ON (t.tgfoid = p.oid)
        WHERE r.relname = ? AND t.tgname = ? AND t.tgconstraint = 0');
    $self->{list_expr} = $class->expr('SELECT t.tgname FROM pg_catalog.pg_trigger AS t
        LEFT JOIN pg_catalog.pg_class AS r ON (t.tgrelid = r.oid) WHERE r.relname = ?');

    $self->{rename_expr} = 'ALTER TRIGGER %s ON %s RENAME TO %s';
    $self->{drop_expr} = 'DROP TRIGGER %s ON %s';

    $self->{template} = {
        'create.sql' => '---
events:
    - insert
    - update
    - delete
for_each: row
when: before
',
    };

    bless $self, $class;
}

sub get
{
    my $self = shift;
    my ($table, $name) = @_;

    unless ($self->SUPER::get($table, $name))
    {
        my $data = $self->one_row($self->{get_expr}, $table, $name);
        return unless $data;

        my $result = {};

        $data->{events} = [ grep $_, @{$data->{events}} ];

        $result->{handler} = \"../../../../functions/$data->{handler}";
        delete $data->{handler};

        $result->{'create.sql'} = delete($data->{'create.sql'});
        $result->{struct} = $self->dump($data);

        return $result;
    }
}

sub list
{
    my $self = shift;
    my ($table) = @_;
    return [ @{$self->all_col($self->{list_expr}, $table)}, @{$self->SUPER::list($table)} ];
}

sub drop
{
    my $self = shift;
    my ($table, $name) = @_;
    $self->SUPER::drop($table, $name) or $self->do($self->{drop_expr}, [$name, $table]);
}

sub store
{
    my $self = shift;
    my ($table, $name, $data) = @_;
    my $struct = $self->load($data->{struct});

    my $when     = uc($struct->{when});
    my $for_each = uc($struct->{for_each});
    my $handler  = ${$data->{handler}};
    $handler =~ s#^\.\./\.\./functions/##;

    local $" = ' OR ';
    my $sql = "CREATE TRIGGER $name $when @{$struct->{events}} ON $table FOR EACH $for_each EXECUTE PROCEDURE $handler";
    $self->drop($table, $name) and $self->do($sql);
}

sub rename
{
    my $self = shift;
    my ($table, $name, $newname) = @_;
    $self->SUPER::rename($table, $name, $newname)
        or $self->do($self->{rename_expr}, [$name, $table, $newname]);
}

1;

__END__

=back

=begin testing SETUP

#!class FusqlFS::Backend::PgSQL::Table::Test

my $new_trigger = {};

=end testing

