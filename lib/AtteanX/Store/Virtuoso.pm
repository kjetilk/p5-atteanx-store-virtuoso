COMMENCE module
use 5.010001;
use strict;
use warnings;

package AtteanX::Store::Virtuoso;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001';

use Moo;
use Types::Standard -types;
use namespace::sweep;

with 'Attean::API::QuadStore';
with 'MooX::Log::Any';

has 'dsn' => (is => 'ro', required => 1, isa => Str);
has 'dbuser' => (is => 'ro', isa => Str, default => 'dba');
has 'dbpasswd' => (is => 'ro', isa => Str, default => 'dba');

has '_dbh' => (is => 'lazy');

has 'turtle_files' => (is => 'lazy', isa => ArrayRef[Str]);

has 'graph' => (is => 'ro', isa => InstanceOf[Attean::IRI])

has 'base_uri' => (is => 'ro', isa => InstanceOf[Attean::IRI]);

has 'parse_flags' => (is => 'ro', isa => Int);

sub BUILD {
	my ($self, $args) = @_;
	$self->_dbh; # Connect to the database
	if ($args->{turtle_files} {
		$self->turtle_files;
	}
}

sub _build__dbh {
	my $self = shift;
	$self->log->debug('Connecting to Virtuoso database with DSN: \'dbi:ODBC:DSN=' . $self->dsn . '\', Username: \'' . $self->dbuser . '\', Password: \'', $self->dbpasswd);
	my $dbh = DBI->connect('dbi:ODBC:DSN=' . $self->dsn, $self->dbuser, $self->dbpasswd);
	warn ref($dbh);
	unless ($dbh) {
		croak 'Couldn\'t connect to database: ' . DBI->errstr;
	}
	return $dbh;
}


sub _build_turtle_files {
	my $self = shift;
}

sub get_quads {
	my $self = shift;
}

1;

__END__

=pod

=encoding utf-8

=head1 NAME

AtteanX::Store::Virtuoso - a module that does something-or-other

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 BUGS

Please report any bugs to
L<http://rt.cpan.org/Dist/Display.html?Queue=AtteanX-Store-Virtuoso>.

=head1 SEE ALSO

=head1 AUTHOR

Kjetil Kjernsmo E<lt>kjetilk@cpan.orgE<gt>.

=head1 COPYRIGHT AND LICENCE

This software is copyright (c) 2016 by Kjetil Kjernsmo.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.


=head1 DISCLAIMER OF WARRANTIES

THIS PACKAGE IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.

