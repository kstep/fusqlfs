package FusqlFS::Version;

our $VERSION = "0.008";

=head1 NAME

FusqlFS::Version - dummy FusqlFS package to store FusqlFS version in a single location

=head1 SYNOPSIS

    use FusqlFS::Version;
    our $VERSION = FusqlFS::Version::VERSION;

=head1 DESCRIPTION

This package is here just to store version number in a single place to avoid
code duplication and make new version building easier.

=cut

1;
