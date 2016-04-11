use Test::Modern;
use Attean;
use Attean::RDF;
use File::Temp qw(tempfile);

{
	my $data = <<'END';
</foo> a </Bar> .
</foo> </bar> </baz> .
</dahut> </category> </Cryptid> .
END

	my ($fh, $filename) = tempfile;
	binmode( $fh, ":utf8" );
	print $fh $data;
	$fh->close;
	my $store = Attean->get_store('Virtuoso')->new(dsn => 'VOSTMP',
																  turtle_files => [$filename],
																  base_uri => iri('http://localhost'));
	isa_ok($store, 'AtteanX::Store::Virtuoso');
	does_ok($store, 'Attean::API::QuadStore');
	is($store->size, 3, 'Reports correct size');
}
