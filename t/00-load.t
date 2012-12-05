#!/usr/bin/env perl -w

use strict;
use warnings;
use Test::More tests => 3;
use Test::NoWarnings;

BEGIN {
	use_ok( 'AnyEvent::Tarantool' );
	use_ok( 'AnyEvent::Tarantool::Queue' );
}

diag( "Testing AnyEvent::Tarantool $AnyEvent::Tarantool::VERSION, Perl $], $^X" );
