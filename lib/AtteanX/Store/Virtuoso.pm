use 5.010001;
use strict;
use warnings;

package AtteanX::Store::Virtuoso;

our $AUTHORITY = 'cpan:KJETILK';
our $VERSION   = '0.001';

use Moo;
use Types::Standard -types;
use namespace::sweep;
use Carp;
use DBI;
use Attean;
use Attean::RDF;

with 'Attean::API::QuadStore';
with 'MooX::Log::Any';

has 'dsn' => (is => 'ro', required => 1, isa => Str);
has 'dbuser' => (is => 'ro', isa => Str, default => 'dba');
has 'dbpasswd' => (is => 'ro', isa => Str, default => 'dba');

has '_dbh' => (is => 'lazy', isa => InstanceOf['DBI::db']);

has 'turtle_files' => (is => 'ro', isa => ArrayRef[Str]);

has 'graph' => (is => 'ro', isa => InstanceOf['Attean::IRI'], coerce => 1, default => sub { return iri('http://example.org/graph') });

has 'base_uri' => (is => 'ro', isa => InstanceOf['Attean::IRI'], predicate => 1, coerce => 1);

has 'parse_flags' => (is => 'ro', isa => Int, predicate => 1, default => 512);

has '_internal_graphs' => (is => 'ro', isa => ArrayRef[Str], 
									default => sub { return ['http://www.w3.org/ns/ldp#',
																	 'http://localhost:8890/DAV/',
																	 'http://localhost:8890/sparql',
																	 'http://www.openlinksw.com/schemas/virtrdf#']});

sub BUILD {
	my ($self, $args) = @_;
	$self->_dbh; # Connect to the database
	foreach my $filename (@{$args->{turtle_files}}) {
		my $sql = "DB.DBA.TTLP_MT (file_to_string_output ('$filename'), '";
		if ($self->has_base_uri) {
			$sql .= $self->base_uri->value;
		}
		$sql .= "', '". $self->graph->value ."'";
		if ($self->has_parse_flags) {
			$sql .= ", " . $self->parse_flags;
		}
		$sql .= ")";
		$self->log->trace("Reading Turtle/N-Quads into Virtuoso using query $sql");
		$self->_dbh->do($sql);
	}
}

sub _build__dbh {
	my $self = shift;
	$self->log->debug('Connecting to Virtuoso database with DSN: \'dbi:ODBC:DSN=' . $self->dsn . '\', Username: \'' . $self->dbuser . '\', Password: \'', $self->dbpasswd);
	my $dbh = DBI->connect('dbi:ODBC:DSN=' . $self->dsn, $self->dbuser, $self->dbpasswd, { LongReadLen => 5000, LongTruncOk => 1 } ); # TODO: May need changing
	unless ($dbh) {
		croak "Couldn't connect to database: " . DBI->errstr;
	}
	return $dbh;
}


sub get_quads {
	my $self = shift;
	my $sqlquery = <<'END';
SELECT __id2i ( "s_1_1-t0"."S" ) AS "s",
  1 AS "sisiri",
  is_bnode_iri_id ( "s_1_1-t0"."S" ) AS "sisblank",
  __id2i ( "s_1_1-t0"."P" ) AS "p",
  __ro2sq ( "s_1_1-t0"."O" ) AS "o",
  is_named_iri_id ( "s_1_1-t0"."O" ) AS "oisiri",
  is_bnode_iri_id ( "s_1_1-t0"."O" ) AS "oisblank",
  (1 - isiri_id ( "s_1_1-t0"."O" )) AS "oisliteral",
  __rdf_sqlval_of_obj /*l*/ ( DB.DBA.RDF_DATATYPE_OF_OBJ (__ro2sq ( "s_1_1-t0"."O" ))) AS "datatype",
  DB.DBA.RDF_LANGUAGE_OF_OBJ (__ro2sq ( "s_1_1-t0"."O" )) AS "lang",
  __id2i ( "s_1_1-t0"."G" ) AS "g"
FROM DB.DBA.RDF_QUAD AS "s_1_1-t0"
WHERE
END
#	$sqlquery .= $self->_internal_graphs . "\nOPTION (QUIETCAST)\n";
	warn $sqlquery;
	my $sth = $self->_dbh->prepare($sqlquery);
		$sth->execute();
		my $ok	= 1;
		my $sub	= sub {
			return unless ($ok);
			if (my $row	= $sth->fetchrow_hashref) {
				return $self->_get_quad(%$row);
			}
			$ok = 0;
			return;
		};
		my $iter	= Attean::CodeIterator->new( generator => $sub, item_type => 'Attean::API::Quad' );
		return $iter;

}

sub _get_quad {
	my ($self, %row) = @_;
#	warn Data::Dumper::Dumper(\%row);
	my $s;
	my $p = iri($row{p});
	my $o;
	my $g = iri($row{g}) || $self->graph;
	if ($row{sisiri}) {
		$s = iri($row{s});
	} elsif ($row{sisblank}) {
		$s = blank($row{s});
	} else {
		croak "Subject $row{s} is neither IRI nor blank";
	}
	if ($row{oisiri}) {
		$o = iri($row{o});
	} elsif ($row{oisliteral}) {
		if ($row{datatype}) {
			$o = dtliteral($row{o}, $row{datatype});
		} elsif ($row{lang}) {
			$o = langliteral($row{o}, $row{lang});
		} else {
			$o = literal($row{o});
		} # TODO: croak if both datatype and language
	} elsif ($row{oisblank}) {
		$o = blank($row{o});
	} else {
		croak "Subject $row{o} is neither IRI, literal nor blank";
	}
	return quad($s, $p, $o, $g);
}

sub size {
	my $self = shift;
	my $sqlquery = 'SELECT count(*) FROM DB.DBA.RDF_QUAD WHERE G NOT IN (__i2idn ( __bft(\''
	  . join('\')) ,  __i2idn ( __bft(\'', @{$self->_internal_graphs})
	  . '\')))';
	my ($size) = $self->_dbh->selectrow_array($sqlquery);
	return $size;
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

