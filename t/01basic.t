=pod

=encoding utf-8

=head1 PURPOSE

Test that AtteanX::Store::Virtuoso compiles.

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

note ("There really aren't any meaningful tests for this module.");
note ("A working Virtuoso install is needed to test it, and it is difficult to do automatically, so the main tests are in xt/");

use_ok('AtteanX::Store::Virtuoso');

done_testing;

