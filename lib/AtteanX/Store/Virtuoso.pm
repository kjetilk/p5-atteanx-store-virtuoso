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

has '_internal_graphs' => (is => 'ro', isa => Str, default => "NOT G IN ( __i2idn ( __bft('http://www.w3.org/ns/ldp#')),\n\t           __i2idn ( __bft('http://localhost:8890/DAV/')),\n\t           __i2idn ( __bft('http://localhost:8890/sparql')),\n\t           __i2idn ( __bft('http://www.openlinksw.com/schemas/virtrdf#')))");

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
	my @out = @_;
	my ($s, $p, $o, $g) = @out;
	my @proj;
	# TODO: sanitize against SQL injection
	my @where;
	if ((!(defined($s)) || $s->is_variable || $s->is_blank)) {
		push(@proj, ('__id2i ( "S" ) AS "s"', 'is_bnode_iri_id ( "S" ) AS "sisblank"'));
	} else {
		# Bound IRI assumed
		push(@where, '"S" = __i2idn ( __bft( \'' . $s->value . '\' ))');
	}
	if ((!(defined($p)) || $p->is_variable || $p->is_blank)) {
		push(@proj, '__id2i ( "P" ) AS "p"');
	} else {
		# Bound IRI assumed
		push(@where, '"P" = __i2idn ( __bft( \'' . $p->value . '\' ))');
	}
	if ((!(defined($o)) || $o->is_variable || $o->is_blank)) {
		push(@proj, ('__ro2sq ( "O" ) AS "o"',
						 'is_named_iri_id ( "O" ) AS "oisiri"',
						 'is_bnode_iri_id ( "O" ) AS "oisblank"',
						 '(1 - isiri_id ( "O" )) AS "oisliteral"',
						 '__rdf_sqlval_of_obj /*l*/ ( DB.DBA.RDF_DATATYPE_OF_OBJ (__ro2sq ( "O" ))) AS "datatype"',
						 'DB.DBA.RDF_LANGUAGE_OF_OBJ (__ro2sq ( "O" )) AS "lang"'));
	} else {
		# TODO: RDF 1.1 vs RDF 1.0 is unclear here, assuming no plain literals
		my $wo = '"O" = ';
		if($o->has_language) {
			$wo .= 'DB.DBA.RDF_MAKE_OBJ_OF_TYPEDSQLVAL ( \'' . $o->value .'\' , __i2id( NULL),  \'' . $o->language .'\' )';
		} else {
# TODO: This might speed up, but not sure
#			if ($o->datatype->equals(iri('http://www.w3.org/2001/XMLSchema#integer')) || $o->datatype->equals(iri('http://www.w3.org/2001/XMLSchema#decimal')) {
#				$wo .= $o->value;
#			} else {
			$wo .= 'DB.DBA.RDF_MAKE_OBJ_OF_TYPEDSQLVAL ( \'' . $o->value .'\' , __i2id( UNAME\'' . $o->datatype->value . '\' ),  NULL)';
#			}
		}
		push(@where, $wo);
	}
	if ((!(defined($g)) || $g->is_variable || $g->is_blank)) {
		push(@proj, '__id2i ( "G" ) AS "g"');
	} else {
		# Bound IRI assumed
		push(@where, '"G" = __i2idn ( __bft( \'' . $g->value . '\' ))');
	}

	my $sqlquery = "SELECT\n\t" . join(",\n\t", @proj) . "\nFROM DB.DBA.RDF_QUAD\nWHERE\n\t";
	if (scalar @where > 0) {
		$sqlquery .= join("\nAND\n\t", @where);
		$sqlquery .= "\nAND\n\t";
	}
	$sqlquery .= $self->_internal_graphs;
	$sqlquery .= "\nOPTION (QUIETCAST)\n";

	$self->log->debug("Preparing query:\n$sqlquery");
	my $sth = $self->_dbh->prepare($sqlquery);
	$sth->execute();
	my $ok	= 1;
	my $sub	= sub {
		return unless ($ok);
		if (my $row	= $sth->fetchrow_hashref) {
			return $self->_get_quad($row, @out);
			}
		$ok = 0;
		return;
	};
	my $iter	= Attean::CodeIterator->new( generator => $sub, item_type => 'Attean::API::Quad' );
	return $iter;
}

sub get_graphs {
	my $self = shift;
	my $sth = $self->_dbh->prepare('SELECT DISTINCT __id2i ( "G" ) AS "g" FROM DB.DBA.RDF_QUAD WHERE ' . $self->_internal_graphs);
	$sth->execute();
	my $ok	= 1;
	my $sub	= sub {
		return unless ($ok);
		if (my $row	= $sth->fetchrow_hashref) {
			return iri($row->{g});
		}
		$ok = 0;
		return;
	};
	my $iter	= Attean::CodeIterator->new( generator => $sub, item_type => 'Attean::API::Term' );
	return $iter;
}


sub _get_quad {
	my $self = shift;
#	warn Data::Dumper::Dumper(\@_);
	my $row = shift;
	my ($s, $p, $o, $g) = @_;
	# The idea here is that if the row returned from the database
	# contains an entry for each of the subject, predicate, object or
	# graph, then it has been a result of a variable, and so that is
	# what we will return. If it isn't, then we have a bound term,
	# which has been passed in the @out array to this method, and then
	# we can reuse that object

	if ($row->{s}) {
		if ($row->{sisblank}) {
			$s = blank($row->{s});
		} else {
			$s = iri($row->{s});
		}
	}
	if ($row->{p}) {
		$p = iri($row->{p});
	}
	if ($row->{o}) {
		if ($row->{oisblank}) {
			$o = blank($row->{o});
		} elsif ($row->{oisiri}) {
			$o = iri($row->{o});
		} elsif ($row->{oisliteral}) {
			if ($row->{datatype}) {
				$o = dtliteral($row->{o}, $row->{datatype});
			} elsif ($row->{lang}) {
				$o = langliteral($row->{o}, $row->{lang});
			} else { # TODO: This shouldn't really happen, with RDF 1.1 semantics?
				$o = literal($row->{o});
			} # TODO: croak if both datatype and language
		}
	}
	if ($row->{g}) {
		$g = iri($row->{g});
	}
	my $q = quad($s, $p, $o, $g);
#	warn $q->as_string;
	return $q;
}

sub size {
	my $self = shift;
	my $sqlquery = 'SELECT count(*) FROM DB.DBA.RDF_QUAD WHERE ' . $self->_internal_graphs;
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

