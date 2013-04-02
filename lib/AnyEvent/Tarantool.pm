package AnyEvent::Tarantool;


#package AnyEvent::Tarantool::Space;

use 5.008008;
use strict;
no warnings 'uninitialized';
use Carp;

use Scalar::Util 'weaken';
use Protocol::Tarantool ':constant';
use Protocol::Tarantool::Spaces;

use Scalar::Util 'weaken';
use AnyEvent::Socket;

use Errno qw(EAGAIN EINTR);
use AnyEvent::Util qw(WSAEWOULDBLOCK);

use AnyEvent::DNS;
use List::Util qw(min);

use constant {
	INITIAL           => 1,
	CONNECTING     => 2,
	CONNECTED      => 4,
	DISCONNECTING  => 8,
	RECONNECTING   => 16,
	RESOLVE        => 32,
	
	MAX_READ_SIZE  => 128*1024,
};

use Data::Dumper;

sub xd ($;$) {
	if( eval{ require Devel::Hexdump; 1 }) {
		no strict 'refs';
		*{ caller().'::xd' } = \&Devel::Hexdump::xd;
	} else {
		no strict 'refs';
		*{ caller().'::xd' } = sub($;$) {
			my @a = unpack '(H2)*', $_[0];
			my $s = '';
			for (0..$#a/16) {
				$s .= "@a[ $_*16 .. $_*16 + 7 ]  @a[ $_*16+8 .. $_*16 + 15 ]\n";
			}
			return $s;
		};
	}
	goto &{ caller().'::xd' };
}



=head1 NAME

AnyEvent::Tarantool - ...

=cut

our $VERSION = '0.05'; $VERSION = eval($VERSION);

=head1 SYNOPSIS

    package Sample;
    use AnyEvent::Tarantool;

    ...

=head1 DESCRIPTION

    ...

=cut


=head1 METHODS

=over 4

=cut

#use Encode;


my %SEEN;
sub clone($);
sub clone($) {
	my $ref = shift;
	exists $SEEN{0+$ref} and warn("return seen $ref: $SEEN{0+$ref}"),return $SEEN{0+$ref};
	local $SEEN{0+$ref};
	if ( UNIVERSAL::isa( $ref, 'HASH' ) ) {
		$SEEN{0+$ref} = my $new = {};
		%$new = map { ref() ? clone($_) : $_ } %$ref;
		bless $new, ref $ref if ref $ref ne 'HASH';
		return $new;
	}
	elsif ( UNIVERSAL::isa( $ref, 'ARRAY' ) ) {
		$SEEN{0+$ref} = my $new = [];
		@$new = map { ref() ? clone($_) : $_ } @$ref;
		bless $new, ref $ref if ref $ref ne 'ARRAY';
		return $new;
	}
	elsif ( UNIVERSAL::isa( $ref, 'SCALAR' ) ) {
		my $copy = $$ref;
		$SEEN{0+$ref} = my $new = \$copy;
		bless $new, ref $ref if ref $ref ne 'SCALAR';
		return $new;
	}
	elsif ( UNIVERSAL::isa( $ref, 'REF' ) ) {
		my $copy;
		$SEEN{0+$ref} = my $new = \$copy;
		$copy = clone( $$ref );
		bless $new, ref $ref if ref $ref ne 'REF';
		return $new;
	}
	elsif ( UNIVERSAL::isa( $ref, 'LVALUE' ) ) {
		my $copy = $$ref;
		my $new = \$copy;
		bless $new, ref $ref if ref $ref ne 'LVALUE';
		return $new;
	}
	else {
		die "Cloning of ".ref( $ref )." not supported";
	}
}

sub init {
	my $self = shift;
	$self->{slab_cache_time} = 1 unless exists $self->{slab_cache_time};
	$self->{debug} ||= 0;
	$self->{timeout} ||= 5;
	$self->{reconnect} = 0.1 unless exists $self->{reconnect};
	
	$self->{spaces} = Protocol::Tarantool::Spaces->new( clone( delete $self->{spaces} ) );
	$self->{seq}  = 0;
	$self->{req} = {};
	
	if (exists $self->{server}) {
		($self->{host},$self->{port}) = split ':',$self->{server},2;
	} else {
		$self->{server} = join ':',$self->{host},$self->{port};
	}
}

sub new {
	my $pk = shift;
	my $self = bless {
		host      => '0.0.0.0',
		port      => '33013',
		read_size =>  4096,
		max_read_size => MAX_READ_SIZE,
		@_,
		req     => {},
		state   => INITIAL,
	}, $pk;
	$self->init();
	return $self;
}

sub _resolve {
	weaken( my $self = shift ); my $cb = shift;
	$self->{_}{resolve} = AnyEvent::DNS::resolver->resolve($self->{host}, 'a', sub {
		$self or return;
		if (@_) {
			my @addrs; my $ttl = 2**32;
			for my $r (@_) { #  [$name, $type, $class, $ttl, @data],
				$ttl = min( $ttl, time + $r->[3] );
				push @addrs, $r->[4];
			}
			$self->{addrs} = \@addrs; $self->{addrs_ttr} = $ttl;
			warn "Resolved $self->{host} into @addrs\n" if $self->{debug};
			$cb->(1);
		} else {
			$cb->(undef, "Not resolved `$self->{host}' ".($!? ": $!" : ""));
		}
	});
}

sub connect {
	weaken( my $self = shift );
	my $cb;$cb = pop if @_ and ref $_[-1] eq 'CODE';
	$self->{state} == CONNECTING and return;
	$self->state( CONNECTING );
	warn "Connecting to $self->{host}:$self->{port} with timeout $self->{timeout} (by @{[ (caller)[1,2] ]})...\n" if $self->{debug};
	my $addr;
	if (my $addrs = $self->{addrs}) {
		if (time > $self->{addrs_ttr}) {
			warn "TTR $self->{addrs_ttr} expired (".time.")\n" if $self->{debug} > 1;
			delete $self->{addrs};
			$self->_resolve(sub {
				warn "Resolved: @_" if $self->{debug};
				$self or return;
				if (shift) {
					$self->state( INITIAL );
					$self->connect($cb);
				} else {
					$self->_on_connreset(@_);
				}
			});
			return;
		}
		push @$addrs,($addr = shift @$addrs);
		warn "Have addresses: @{ $addrs }, current $addr" if $self->{debug} > 1;
	}
	else {
		if ( $self->{host} =~ /^[\.\d]+$/ and my $paddr = pack C4 => split '\.', $self->{host},4 ) {
			$self->{addrs} = [ $addr = Socket::inet_ntoa( $paddr ) ];
			$self->{addrs_ttr} = 2**32;
			warn "using ip host: $self->{host} ($addr)" if $self->{debug};
		} else {
			warn "Have no addrs, resolve $self->{host}\n" if $self->{debug} > 1;
			$self->_resolve(sub {
				warn "Resolved: @_" if $self->{debug};
				$self or return;
				if (shift) {
					$self->state( INITIAL );
					$self->connect($cb);
				} else {
					$self->_on_connreset(@_);
				}
			});
			return;
		}
	}
	warn "Connecting to $addr:$self->{port} with timeout $self->{timeout} (by @{[ (caller)[1,2] ]})...\n" if $self->{debug};
	$self->{_}{con} = AnyEvent::Socket::tcp_connect
		$addr,$self->{port},
		sub {
			$self or return; pop;
			#warn "Connect: @_...\n";# if $self->{debug};
			
			my ($fh,$host,$port) = @_;
			
			$self->_on_connect($fh,$host,$port,$cb);
		},
		sub {
			$self or return;
			$self->{timeout};
		};
	return;
}

sub _on_connect_success { 0 }

*_connect_check = \&ping;

sub _on_connect {
	my ($self,$fh,$host,$port,$cb) = @_;
	unless ($fh) {
		#warn "Connect failed: $!";
		if ($self->{reconnect}) {
			$self->{connfail} && $self->{connfail}->( $self, "$!" );
		} else {
			$self->{disconnected} && $self->{disconnected}->( $self, "$!" );
		}
		$self->_reconnect_after;
		return;
	}
	$self->{fh} = $fh;
	if ($self->_on_connect_success( $fh,$host,$port )) {
		$self->state( CONNECTED );
	} else {
		my $tm;$tm = AE::timer $self->{timeout},0,sub {
			undef $tm;
			local $! = Errno::ETIMEDOUT;
			local $self->{conncheck} = 1;
			$self->_on_connreset("$!");
		};
		$self->_connect_check(sub { $tm or return;
			my $res = shift;undef $tm;
			if ($res and $res->{code} == 0) {
				$self->state( CONNECTED );
				$self->{connected} && $self->{connected}->( $self, $host,$port );
			}
			else {
				return if $self->{conncheck};
				$self->_on_connreset(@_);
			}
			
		});
	}
	
	weaken(my $c = $self);
	$self->{rw} = AE::io $fh,0,sub {
		my $buf = $self->{rbuf};
		my $len;
		while ( $self and ( $len = sysread( $fh, $buf, $self->{read_size}, length $buf) ) ) {
			$self->{_activity} = $self->{_ractivity} = AE::now;
			if ($len == $self->{read_size} and $self->{read_size} < $self->{max_read_size}) {
				$self->{read_size} *= 2;
				$self->{read_size} = $self->{max_read_size} || MAX_READ_SIZE
					if $self->{read_size} > ($self->{max_read_size} || MAX_READ_SIZE);
			}
			
			my $ix = 0;
			while () {
				last if length $buf < $ix + 12;
				my ($type,$l,$seq) = unpack 'VVV', substr($buf,$ix,12);
				if ( length($buf) - $ix >= 12 + $l ) {
					$c->packet( $type, $seq, \( substr($buf,$ix,$l+12) ) );
					$ix += 12+$l;
				}
				else {
					last;
				}
			}
			$buf = substr($buf,$ix);# if length $buf > $ix;
		}
		return unless $self;
		$self->{rbuf} = $buf;
		
		if (defined $len) {
			warn "EOF from client ($len)";
			$! = Errno::EPIPE;
			$self->_on_connreset("$!");
		} else {
			if ($! == EAGAIN or $! == EINTR or $! == WSAEWOULDBLOCK) {
				return;
			} else {
				warn "Client gone: $!";
				#$! = Errno::EPIPE;
				$self->_on_connreset("$!");
			}
		}
	};
}

sub _on_connreset {
	my ($self,$error) = @_;
	warn "Connection reset ($self->{server}): $error. Have ".(0+keys %{ $self->{req} })." requests waiting";
	#use Data::Dumper;
	#warn Dumper $self->{req};
	#local $self->{actioqueue} = [];
	my %req = %{$self->{req}};
	%{$self->{req}} = ();
	$self->disconnect($error);
	$self->_reconnect_after;
	while (my ($seq,$hdl) = each %req ) {
		#warn "call handler $seq -> [@$hdl]";
		my ($cb) = @$hdl;
		if ($cb) {
			local $@;
			eval{ $cb->(undef, $error); 1 } or warn;
		}
	}
}

sub _reconnect_after {
	weaken( my $self = shift );
	if ($self->{reconnect}) {
		# Want to reconnect
		$self->state( RECONNECTING );
		warn "Reconnecting (state=$self->{state}) to $self->{host}:$self->{port} after $self->{reconnect}...\n" if $self->{debug};
		$self->{timers}{reconnect} = AE::timer $self->{reconnect},0, sub {
			$self or return;
			$self->state(INITIAL);
			$self->connect;
		};
	} else {
		$self->state(INITIAL);
		return;
	}
}

sub reconnect {
	my $self = shift;
	return if $self->{state} == RECONNECTING;
	$self->disconnect(@_);
	$self->state(RECONNECTING);
	$self->connect;
}

sub disconnect {
	my $self = shift;
	$self->state(DISCONNECTING);
	warn "Disconnecting (state=$self->{state}, pstate=$self->{pstate}) by @{[ (caller)[1,2] ]}\n" if $self->{debug};
	if ( $self->{pstate} &(  CONNECTED | CONNECTING ) ) {
		delete $self->{con};
	}
	delete $self->{ww};
	delete $self->{rw};
	
	delete $self->{_};
	delete $self->{timers};
	delete $self->{fh};
	if ( $self->{pstate} == CONNECTED ) {
			$self->{disconnected} && $self->{disconnected}->( $self, @_ );
	}
	elsif ( $self->{pstate} == CONNECTING or $self->{pstate} == RECONNECTING ) {
		$self->{connfail} && $self->{connfail}->( $self, @_ );
	}
	return;
}

sub state {
	my $self = shift;
	$self->{pstate} = $self->{state} if $self->{pstate} != $self->{state};
	$self->{state} = shift;
}

sub _hash_flags_to_mask($) {
	return $_[0] || 0 unless ref $_[0];
	my $flags = shift;
	
	if (ref $flags eq 'ARRAY') {
		my %flags; @flags{ @$flags } = (1) x @$flags;
		$flags = \%flags;
	}
	elsif (ref $flags eq 'HASH') {
		# ok
	}
	else {
		croak "Flags could be bitmask or hashref or arrayref";
	}
	return +
		( $flags->{return} ? TNT_FLAG_RETURN : 0 )
		|
		( $flags->{ret} ? TNT_FLAG_RETURN : 0 )
		|
		( $flags->{add} ? TNT_FLAG_ADD : 0 )
		|
		( $flags->{replace} ? TNT_FLAG_REPLACE : 0 )
		|
		( $flags->{rep} ? TNT_FLAG_REPLACE : 0 )
		|
		( $flags->{quiet} ? TNT_FLAG_BOX_QUIET : 0 )
		|
		( $flags->{notstore} ? TNT_FLAG_NOT_STORE : 0 )
	;
}

sub _handle_packet {}
sub packet {
	weaken (my $self = shift); $self or return;
	my ($type,$id,$pk) = @_;
	my $format;
	if ($type == TNT_INSERT or $type == TNT_SELECT or $type == TNT_DELETE ) {
		$format = $self->{unpack};
	}
	#warn "Received packet type=$type, id=$id, size=".(length($$pk)-12);
	#warn xd "$$pk";
	if ( exists $self->{req}{ $id } ) {
		my ($cb, $space, $format, $args) = @{ delete $self->{req}{ $id } };
		#warn "response: $pk, $format, $space (cb=$cb)";
		my $pkt = eval{
			local $SIG{__WARN__} = sub {
				local $_ = shift;
				s{(?<=\S)\s+at .+? \d+\.\s+$}{}s;
				local $@;
				eval {
					#if (m{Field i should be of size}) {
						if ($format) {
							warn "[AE::TNT] [$_] format [@$format] {".join(' ',unpack '(H2)*', substr($$pk,0,12))."}...\n";
						}
						elsif($space) {
							warn "[AE::TNT] [$_] space:$space->{id} format [$space->{unpack}, $space->{default_unpack}[0]] {".join(' ',unpack '(H2)*', substr($$pk,0,12))."}...\n";
						}
						else {
							warn "[AE::TNT] [$_] no format {".join(' ',unpack '(H2)*', substr($$pk,0,12))."}...\n";
						}
					#}
				};
				warn if $@;
			};
			Protocol::Tarantool::response( $pk, $format ? ($format->[0],$format->[1]) : $space ? ( $space->{unpack}, $space->{default_unpack}[0] ) : () )
		};
		$self->_handle_packet( $pkt, $args );
		if ($pkt) {
			if ($pkt->{code} == 0) {
				$cb->($pkt);
			} else {
				#warn "packet\n".xd("$pk")." failed: $pkt->{errstr}";
				$cb->(undef, $pkt->{errstr}, $pkt);
			}
		} else {
			#warn "packet\n".xd("$pk")." decoding failed: $@";
			$cb->(undef, my $e = $@);
		}
	} else {
		my $pkt = eval{ Protocol::Tarantool::response( $pk ) };
		if ($self->{unexpected}) {
			$self->{unexpected}->($pkt);
		} else {
			warn "Unexpected response packet received: ".Dumper( $pkt );
		}
	}
}

sub write {
	my ($self,$buf,$cb) = @_;
	$self->{fh} or return $cb->(undef, "Not connected");
	if ( $self->{wbuf} ) {
		${ $self->{wbuf} } .= $$buf;
		return;
	}
	#warn "Sending ".xd $$buf;
	my $w = syswrite( $self->{fh}, $$buf );
	if ($w == length $$buf) {
		# ok;
	}
	elsif (defined $w) {
		$self->{wbuf} = \( substr($$buf,$w) );
		$self->{ww} = AE::io $self->{fh}, 1, sub {
			$w = syswrite( $self->{fh}, ${ $self->{wbuf} } );
			if ($w == length ${ $self->{wbuf} }) {
				delete $self->{wbuf};
				delete $self->{ww};
			}
			elsif (defined $w) {
				substr( ${ $self->{wbuf} }, 0, $w, '');
			}
			else {
				#warn "disconnect: $!";
				$self->_on_connreset("$!");
			}
		};
	}
	else {
		$self->_on_connreset("$!");
	}
}


sub ping : method {
	my $self = shift;
	my $cb = pop;
	
	eval {
		my $pk =  Protocol::Tarantool::ping( ++$self->{seq} );
		$self->{req}{ $self->{seq} } = [ $cb ];
		$self->write( \$pk, $cb );
	1} or do {
		$cb->(undef, my $e = $@);
	};
}

=item select( $space, $keys, [ $opts = { index = 0, limit = 2**32, offset = 0 }, ] $cb->( $result, $error ) )

=over 4

=item C<$space> may be either id or alias, or object

=item C<$keys> may be either nonref value (for ex: C<123>), or Array of Arrays. AoA is canonical value and always preferred to avoid ambiguity

    $c->select( 0, 1, $cb )
      # is equivalent to $c->select( 0, [[1]], $cb )
    
    $c->select( 0, [[1]], $cb )
      # select one tuple from space 0 with index0 key value 1
      
    $c->select( 3, [[1,2],[3,4]], $cb )
      # select 2 tuples from space 3 with index0 key values [1,2] for the first and [3,4] for the second 

    $c->select( 2, [[1,2,3]], { index => 2, limit => 10, offset => 1  }, $cb )
      # select tuples from space 2 with index2 key values [1,2,3], at most 10 records, skip first record
i
=back

=cut

sub select : method { #( space, keys, [ { index, limit, offset } ] , cb )
	my $self = shift;
	my $cb   = pop;
	eval {
		my $sno = shift;
		my $keys = shift;
		my $space = $self->{spaces}->space( $sno );
		#warn "space = ".dumper $space;
		my $opts = shift || {};
		
		my $index = $space->index( $opts->{index} || 0 );
		($keys,my $format) = $space->keys( $keys, $index );
		
		# req_id, ns, idx, offset, limit, keys, [ format ]
		#warn "select from space no = $space->{no} @{ $keys }";
		my $pk = Protocol::Tarantool::select(
			++$self->{seq},
			$space->{no},
			$index->{no},
			$opts->{offset} || 0,
			$opts->{limit} || 0xFFFFFFFF,
			$keys,
			$format,
		);
		#warn "Created select: \n".xd $pk;
		$self->{req}{ $self->{seq} } = [ $cb, $space ];
		$self->write( \$pk, $cb );
	1} or do {
		$cb->(undef, my $e = $@);
	};
	#my $space = $self->{spaces}->find( $_[0] );
}

=item delete( $space, $key, [ $opts = { ret = 0, }, ] $cb->( $result, $error ) )

=over 4

=item C<$space> may be either id or alias, or object

=item C<$key> may be either nonref value (for ex: C<123>), or Array. Array is canonical value and always preferred to avoid ambiguity

    $c->delete( 0, 1, $cb )
      # is equivalent to $c->delete( 0, [1], $cb )
    
    $c->delete( 0, [1], $cb )
      # delete one tuple from space 0 with index0 key value 1
      
    $c->delete( 3, [1,2], $cb )
      # delete 1 tuple from space 3 with index0 key value [1,2]

    my $tuple = $c->delete( 2, [1,2,3], { ret => 1 }, $cb )
      # delete tuple from space 2 with index0 key value [1,2,3], and return deleted record

=back

=cut

sub delete : method { #( space, primary-key, cb )
	my $self = shift;
	my $cb   = pop;
	eval {
		my $sno   = shift;
		my $key   = shift;
		my $flags = _hash_flags_to_mask( shift );
		my $space = $self->{spaces}->space( $sno );
		#warn "space = ".dumper $space;
		
		my ($keys,$format) = $space->keys( [ $key ], 0, 1 );
		@$keys != 1 and croak "delete takes only one primary key";
		
		# req_id, ns, idx, offset, limit, keys, [ format ]
		my $pk = Protocol::Tarantool::delete(
			++$self->{seq},
			$space->{no},
			$flags,
			$keys->[0],
			$format,
		);
		#warn "Created delete: \n".xd $pk;
		$self->{req}{ $self->{seq} } = [ $cb, $space ];
		$self->write( \$pk, $cb );
	1} or do {
		$cb->(undef, my $e = $@);
	};
}

=item update( $space, $key, [ $update, $update, ... ] [ $opts = { ret = 0, }, ] $cb->( $result, $error ) )

=over 4

=item C<$space>
may be either id or alias, or object

=item C<$key>
may be either nonref value (for ex: C<123>), or Array. Array is canonical value and always preferred to avoid ambiguity

=item C<$update>
is an arrayref, one of:

=over 4

=item B<C< '=' >> C<[set]( $new_value )> Set field to new value

=item B<C< ':' >> C<[splice] ( $offset, $length [, $new_value ] )> Start at offset, cut length bytes, and add a new value

=item B<C< '!' >> C<[insert] ( $new_value )> insert a field (before the one specified).

=item B<C< '#' >> C<[delete] ()> delete a field

=item B<C< '+' >> C<[add] ( $int_value )> Add int_value to field. Field and value must be INT32 or INT64

=item B<C< '&' >> C<[and] ( $int_value )> Binary and int_value with field. Field and value must be INT32 or INT64

=item B<C< '|' >> C<[or] ( $int_value )> Binary or int_value with field. Field and value must be INT32 or INT64

=item B<C< '^' >> C<[xor] ( $int_value )> Binary xor int_value with field. Field and value must be INT32 or INT64

=back

    # For update operations space fields must be configured

    $c->update( 0, 1, [...] $cb )
      # is equivalent to $c->update( 0, [1], [...], $cb )
    
    $c->update( 0, [1], [ [ fieldX => '=', 123  ] ], $cb )
      # update tuple from space 0 with index0 key value 1, set field, configured as fieldX, to value 123
      
    $c->update( 0, [1], [ [ fieldX => ':', 1,1, "test"  ] ], $cb )
      # update tuple, splice fieldX from offset 1, length 1 ,and add "test";

=back

=cut

sub update : method { #( space, primary-key, cb )
	my $self = shift;
	my $cb   = pop;
	eval {
		my $sno   = shift;
		my $key   = shift;
		my $ops   = shift;
		my $flags = _hash_flags_to_mask( shift );
		
		my $space = $self->{spaces}->space( $sno );
		
		my ($keys,$format) = $space->keys( [ $key ], 0, 1 );
		$ops = $space->updates( $ops );
		@$keys != 1 and croak "delete takes only one primary key";
		
		# req_id, ns, idx, offset, limit, keys, [ format ]
		my $pk = Protocol::Tarantool::update(
			++$self->{seq},
			$space->{no},
			$flags,
			$keys->[0],
			$ops,
			$format,
		);
		#warn "Created update: \n".xd $pk;
		$self->{req}{ $self->{seq} } = [ $cb, $space ];
		$self->write( \$pk, $cb );
	1} or do {
		$cb->(undef, my $e = $@);
	};
}

=item insert( $space, $tuple, [ $opts = { add = 0, replace = 0, return = 0 }, ] $cb->( $result, $error ) )

=over 4

=item C<$space> may be either id or alias, or object

=item C<$tuple> must be array

    $c->insert( 0, [1,2,"test"], $cb )
      # insert tuple into space 0
      
    $c->insert( 3, [1,2], { add => 1, return => 1 } $cb )
      # insert tuple into space 3 only if it not exists. return tuple back

=back

=cut

sub insert : method { #( space, tuple [,flags], cb )
	my $self = shift;
	my $cb   = pop;
	eval {
		my $sno   = shift;
		my $tuple = shift;
		my $flags = _hash_flags_to_mask( shift );
		
		my $space = $self->{spaces}->space( $sno );
		($tuple,my $format) = $space->tuple( $tuple );
		
		# req_id, ns, idx, offset, limit, keys, [ format ]
		++$self->{seq};
		my $pk = Protocol::Tarantool::insert(
			$self->{seq},
			$space->{no},
			$flags,
			$tuple,
			$format,
		);
		#warn "Created insert: \n".xd $pk;
		$self->{req}{ $self->{seq} } = [ $cb, $space ];
		$self->write( \$pk, $cb );
	1} or do {
		$cb->(undef, my $e = $@);
	};
}

=item lua( $function, $args_tuple, [ $opts = { in, out }, ] $cb->( $result, $error ) )

=over 4

=item C<$function> is a name of lua function, defined in Tarantool instance

=item C<$args_tuple> must be array

=item C<$opts.in> Format string for arguments.

=item C<$opts.out> Format string for return value.

=over 4

=item p Binary string (string w/o utf8 flag). Default format. Could be unpacked manually with C<unpack>

=item u Unicode (utf-8) string.

=item L (l) Unsigned (signed) 64-bit integer. Works only if perl was built with int64 support

=item I (i) Unsigned (signed) 32-bit integer.

=item S (s) Unsigned (signed) 16-bit integer.

=item C (c) Unsigned (signed) 8-bit integer.

=back

    $c->lua( 'os.time', [], { out => 'I' } $cb )
      # call lua os.time(). Return value unpacked as 32bit unsigned integer
    
=back

=cut


sub lua : method { #( space, tuple [,flags], cb )
	my $self = shift;
	my $cb   = pop;
	eval {
		my $proc   = shift;
		my $tuple = shift;
		my $opts = shift;
		my $informat  = ref $opts eq 'HASH' ? $opts->{in} : '';
		my $outformat = ref $opts eq 'HASH' ? $opts->{out} : '';
		my $flags = _hash_flags_to_mask( $opts );
		
		# req_id, ns, idx, offset, limit, keys, [ format ]
		my $pk = Protocol::Tarantool::lua(
			++$self->{seq},
			$flags,
			$proc,
			$tuple,
			$informat,
#			$format,
		);
		#warn "Created lua $proc. id=$self->{seq}: \n".xd $pk;
		$self->{req}{ $self->{seq} } = [ $cb, undef, [ $outformat ], exists $opts->{args} ? ( $opts->{args} ) : () ];
		#warn dumper $self->{req};
		$self->write( \$pk, $cb );
	1} or do {
		$cb->(undef, my $e = $@);
	};
}

sub luado : method { #( code, [,flags] cb )
	my $self = shift;
	my $cb   = pop;
	eval {
		my $code = shift;
		my $opts = shift;
		my $informat  = ref $opts eq 'HASH' ? $opts->{in} : '';
		my $outformat = ref $opts eq 'HASH' ? $opts->{out} : '';
		my $flags = _hash_flags_to_mask( $opts );
		
		# req_id, ns, idx, offset, limit, keys, [ format ]
		my $pk = Protocol::Tarantool::lua(
			++$self->{seq},
			$flags,
			"box.dostring",
			[$code],
			$informat,
#			$format,
		);
		my $xcb = sub {
			if ( $_[0] and @{ $_[0]{tuples} } ) {
				$cb->(1, @{ $_[0]{tuples}[0] });
			}
			else {
				$cb->(undef, @_);
			}
		};
		#warn "Created lua $proc. id=$self->{seq}: \n".xd $pk;
		$self->{req}{ $self->{seq} } = [ $xcb, undef, [ $outformat ], exists $opts->{args} ? ( $opts->{args} ) : () ];
		#warn dumper $self->{req};
		$self->write( \$pk, $xcb );
	1} or do {
		$cb->(undef, my $e = $@);
	};
}

sub slab_info {
	my $self = shift;
	my $cb   = pop;
	if ($self->{slab_info} and $self->{slab_info}{until} > time) {
		return $cb->( $self->{slab_info}{slab} );
	}
	elsif ($self->{slab_info} and $self->{slab_info}{queue}) {
		push @{$self->{slab_info}{queue}}, $cb;
		return;
	}
	else {
		$self->{slab_info} = {};
		$self->{slab_info}{queue} = [ $cb ];
		$self->luado(q{
			if aetnt == nil then
				aetnt = {}
				aetnt.stats = function()
					local st;
					if (aetnt.lstat == nil) then
						st = {
							call    = 0;
							select  = 0;
							update  = 0;
							delete  = 0;
							replace = 0;
						}
					else
						st = {
							call    = box.stat.CALL.total - aetnt.lstat.call;
							select  = box.stat.SELECT.total - aetnt.lstat.select;
							update  = box.stat.UPDATE.total - aetnt.lstat.update;
							delete  = box.stat.DELETE.total - aetnt.lstat.delete;
							replace = box.stat.REPLACE.total - aetnt.lstat.replace;
						}
					end
					aetnt.lstat = {
						call    = box.stat.CALL.total;
						select  = box.stat.SELECT.total;
						update  = box.stat.UPDATE.total;
						delete  = box.stat.DELETE.total;
						replace = box.stat.REPLACE.total;
					}
					return st
				end
			end
			local r = {
				'arena.used', box.slab().arena_used,
				'arena.size', box.slab().arena_size,
				'info.lsn',   box.info().lsn,
				'info.lag',   tostring(box.info().recovery_lag)
			};
			local stats = aetnt.stats()
			for k,v in pairs(stats) do
				r[#r+1] = 'stat.' .. k
				r[#r+1] = tonumber( v )
			end
			if box.info().recovery_last_update > 0 then
				r[#r + 1] = 'info.lut'
				r[#r + 1] = tostring(box.time() - box.info().recovery_last_update)
			end
			local s = box.slab().slabs;
			local items_used = 0;
			for k,i in pairs(s) do
				for x,v in pairs(i) do
					r[#r + 1] = 'slab.' .. k .. '.' .. x
					r[#r + 1] = tonumber( v )
					if x == 'bytes_used' then
						items_used = items_used + tonumber(v)
					end
				end
			end
			r[#r + 1] = 'arena.items'
			r[#r + 1] = items_used
			for k,v in pairs(box.space) do
				if v.enabled then
					r[#r+1] = 'space[' .. k ..  '].items'
					r[#r+1] = v:len()
				end
			end
			return r
		}, sub {
			if ( shift ) {
				my %s = @_;
				$_ = unpack +( length == 8 ? 'Q' : length == 4 ? 'V' : 'a*' ), $_ for ( values %s ) ;
				my %slab;
				my $items = 0;
				my $isize = 0;
				my $maxitems = 0;
				my $maxsize = 0;
				my %arena;
				my %space;
				my %info;
				my %stat;
				for (keys %s) {
					my ($k,$sz);
					if( ($sz,$k) = m{^slab\.(\d+)\.(.+)$} ) {
						$slab{$sz}{$k} = $s{$_};
						if ( $k eq 'items' ) {
							$items += $s{$_};
							$maxitems = $s{$_} if $maxitems < $s{$_};
						};
						if ( $k eq 'bytes_used' ) {
							$isize += $s{$_};
							$maxsize = $s{$_} if $maxsize < $s{$_};
						}
					}
					elsif( ($k) = m{^arena\.(.+)$} ) {
						$arena{$k} = $s{$_};
					}
					elsif( ($k) = m{^info\.(.+)$} ) {
						$info{$k} = $s{$_};
					}
					elsif( ($k) = m{^stat\.(.+)$} ) {
						$stat{$k} = $s{$_};
					}
					elsif( ($k) = m{^space\[(\d+)\]\.items$} ) {
						$space{$k} = $s{$_};
					}
					else {
						warn "unknown key: $_";
					}
				}
				my $maxbtotal;
				for (keys %slab) {
					$slab{$_}{bytes_total} = $slab{$_}{bytes_used} + $slab{$_}{bytes_free};
					$maxbtotal = $slab{$_}{bytes_total} if $maxbtotal < $slab{$_}{bytes_total};
				}
				$arena{free} = $arena{size} - $arena{used};
				$self->{slab_info}{until} = $self->{slab_cache_time} ? time + $self->{slab_cache_time} : 0;
				$self->{slab_info}{slab} = {
					items     => $items,
					maxitems  => $maxitems,
					size      => $isize,
					maxsize   => $maxsize,
					maxbtotal => $maxbtotal,
					slab      => \%slab,
					arena     => \%arena,
					space     => \%space,
					info      => \%info,
					stat      => \%stat,
				};
			} else {
				warn "[TNT][ERR] @_";
				$self->{slab_info}{until} = $self->{slab_cache_time} ? time + 1 : 0;
				$self->{slab_info}{slab} = undef;
			}
			$_->( $self->{slab_info}{slab} ) for @{ delete $self->{slab_info}{queue} };
		});
	}
}


=back

=head1 AUTHOR

Mons Anderson, C<< <mons@cpan.org> >>

=head1 COPYRIGHT & LICENSE

Copyright 2012 Mons Anderson, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

=cut

1;
