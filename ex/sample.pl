#!/usr/bin/env perl

use strict;
use EV;
use AnyEvent::Tarantool;
use Data::Dumper;

my $t = AnyEvent::Tarantool->new(
	host => 'localhost.',
	port => 33013,
	#debug => 1,
	spaces => {
		4 => {
			name   => 'test',
			fields => [qw(a   b   c   e   d   f)],
			types  => [qw(INT STR INT INT INT UTF)],
			indexes => {
				0 => 'a',
			}
		}
	},
	connected => sub {
		my $t = shift;
		warn "connected: @_";
		$t->lua('box.dostring',[ 'return tostring( 100*tonumber(box.slab().arena_used)/tonumber(box.slab().arena_size) ) .. "%"' ], sub {
			if (my $res = shift) {
				warn "arena use: $res->{tuples}->[0][0]";
			} else {
				warn "error: @_";
			}
		});
		$t->insert( 'test', [1, "test", 123 ], { ret => 1, add => 0 }, sub {
			warn "insert: ".Dumper @_;
			my $res = shift;
			if ($res) {
				$t->select(test => [[ 1 ]], sub {
					warn Dumper @_;
				});
			} else {
				warn "Error :@_";
			}
		}) ;
	},
);

$t->connect;
EV::loop;


