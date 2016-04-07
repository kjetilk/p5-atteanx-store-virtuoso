=pod

=encoding utf-8

=head1 PURPOSE

Run standard Test::Attean::TripleStore tests

=head1 AUTHOR

Kjetil Kjernsmo E<lt>kjetilk@cpan.orgE<gt>.

=head1 COPYRIGHT AND LICENCE

This software is copyright (c) 2016 by Kjetil Kjernsmo.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.


=cut

use strict;
use warnings;
use Test::More;
use Test::Roo;
use Attean;

with 'Test::Attean::QuadStore';

sub create_store {
	my $self = shift;
	my %args = @_;
	my $quads = $args{quads} // [];
	my $ser = Attean->get_serializer('NQuads')->new();
	my $data = $ser->serialize_list_to_bytes(@$quads);
	my $store = Attean->get_store('Virtuoso')->new(dsn => 'VOSTMP',
																 turtle_files => #TODO: need tmpfile);

run_me;

done_testing;

