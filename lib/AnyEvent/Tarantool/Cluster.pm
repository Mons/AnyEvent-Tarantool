package AnyEvent::Tarantool::Cluster::Master;
our @ISA = 'AnyEvent::Tarantool';
package AnyEvent::Tarantool::Cluster::Slave;
our @ISA = 'AnyEvent::Tarantool';

package AnyEvent::Tarantool::Cluster;

use 5.008008;
use strict;
use warnings;
no warnings 'uninitialized';
use Carp;
use Scalar::Util 'weaken';
use List::Util 'shuffle';
use AnyEvent::Tarantool;
use Time::HiRes 'time';

sub log_debug {
	my $self = shift;
	$self->{log} or return warn @_;
	$self->{log}->debug(@_);
}

sub log_warn {
	my $self = shift;
	$self->{log} or return warn @_;
	$self->{log}->warn(@_);
}

sub log_error {
	my $self = shift;
	$self->{log} or return warn @_;
	$self->{log}->error(@_);
}


sub new {
	my $pkg = shift;
	my $self = bless {
		timeout => 1,
		recovery_lag  => 1,
		master_class => 'AnyEvent::Tarantool::Cluster::Master',
		slave_class => 'AnyEvent::Tarantool::Cluster::Slave',
		@_,
		stores => [],
	},$pkg;
	
	$self->{expected} = 0;
	for my $srv (@{$self->{servers}}) {
		
		my $weight = $srv->{weight} ||= 1;
		$self->{expected} += $weight;
		
		my $is_master = $srv->{master} ? 1 : 0;
		my $warned;
		my $role = $is_master ? "master" : "slave";
		my $class = $self->{"${role}_class"};
		
		my $t;$t = $class->new(
			timeout => $self->{timeout},
			debug   => $self->{debug},
			%$srv,
			spaces => $self->{spaces},
			connected => sub {
				my ($c,$host,$port) = @_;
				$warned = 0;
				$self->log_debug( "\u$role tarantool connected $host:$port (@{ $self->{stores} } + $c)");
				$self->watchlsn unless $is_master;
				$srv->{connected} = 1;
				$self->db_online( $role => $c, $weight);
			},
			connfail => sub {
				shift if ref $_[0];
				$warned++ or
					$self->log_error("\u$role tarantool connect failed: @_");
				$srv->{connected} = 0;
			
			},
			disconnected => sub {
				shift if ref $_[0];
				@_ and $self->log_warn("\u$role tarantool connect closed: @_");
				$srv->{connected} = 0;
				$self->db_offline( $role => $t );
			}
		);
		$srv->{t} = $t;
	}
	
	$self;
}

sub nodes {
	my $self = shift;
	return @{ $self->{servers} };
}

sub db_online {
	my $self = shift;
	my $key = shift;
	my $c = shift;
	my $weight = shift || 1;
	
	$self->{$key} = $c;
	my $first = @{ $self->{stores} } == 0;
	$self->{stores} = [ shuffle @{ $self->{stores} }, ($c) x $weight ];
	my $event = "${key}_connected";
	$self->{$event} && $self->{$event}( $self,$c );
	$first && $self->{one_connected} && $self->{one_connected}( $self,$c );
	
	if( $self->{expected} == @{ $self->{stores} } ) {
		$self->{all_connected} && $self->{all_connected}( $self, @{ $self->{stores} } );
	}
}

sub db_offline {
	my $self = shift;
	my $key = shift;
	my $c = shift;
	my $xc = delete $self->{$key};
	$self->{stores} = [ shuffle grep $_ != $c, @{ $self->{stores} } ];
	my $event = "${key}_disconnected";
	$self->{$event} && $self->{$event}( $self,$c );
	if( @{ $self->{stores} } == 0 ) {
		delete $self->{watch_timer};
		$self->{all_disconnected} && $self->{all_disconnected}( $self, @{ $self->{stores} } );
	}
	$c->reconnect;
}

sub connect {
	my $self = shift;
	for my $srv (@{$self->{servers}}) {
		$srv->{t}->connect;
	}
	$self->watchdog();
}

sub watchdog {
	my $self = shift;
	my $livetimeout = $self->{timeout};
	my $ids;
	$self->{watchdog} ||= AE::timer 0,($livetimeout/2),sub {
		my $id = ++$ids;
		my %check;
		my $tm;$tm = AE::timer $livetimeout,0,sub {
			undef $tm;
			for ( keys %check ) {
				local $! = Errno::ETIMEDOUT;
				$check{$_}->reconnect("$!");
			}
		};
		for my $srv (@{$self->{servers}}) {
			if ($srv->{t}{state} == AnyEvent::Tarantool::CONNECTED) {
				$check{$srv->{t}{server}} = $srv->{t};
				my $time = time;
				$srv->{t}->ping(sub {
					my $r = shift;
					$time = time - $time;
					if ($r and $r->{code} == 0) {
					} else {
						$self->log_error( "$srv->{t}{server} failed: @_" );
						$srv->{t}->reconnect(@_);
					}
					delete $check{$srv->{t}{server}};
					undef $tm unless %check;
				});
			}
		}
	};
}

sub watchlsn {
	my $self = shift;
	my $livetimeout = $self->{timeout};
	my $ids;
	$self->{watchlsn} ||= AE::timer 0,($livetimeout/2),sub {
		return unless $self->{master};
		my $id = ++$ids;
		my %check;
		$self->{master}->lua("box.dostring", ["return box.info().lsn"], { out => 'L' }, sub {
			my $res = shift;
			if ($res and $res->{count} > 0) {
				my $mlsn = $res->{tuples}[0][0];
				for my $srv (@{$self->{servers}}) {
					if ($srv->{t}{state} == AnyEvent::Tarantool::CONNECTED and !$srv->{master}) {
						$srv->{t}->lua('box.dostring', ['return { box.info.lsn, tostring(0.0+box.info.recovery_lag) }'], { out => 'Lp' },sub {
							my $res = shift;
							if( $res and $res->{status} eq 'ok' and $res->{count} > 0 ){
								my ($lsn,$lag) = @{ $res->{tuples}[0] };
								return if $lsn >= $mlsn;
								return if $lag < $self->{recovery_lag};
								$self->log_error("slave $srv->{t}{server} too slow: $lsn, $lag");
								$srv->{t}->reconnect;
							} else {
								shift;
								$self->log_error( "slave error: @_" );
								$srv->{t}->reconnect(@_);
							}
						});

=for rem
						$srv->{t}->luado('return { box.info.lsn, tostring(0.0+box.info.recovery_lag) }', { out => 'Lp' },sub {
							warn "warchlsn: @_";
							if( shift ){
								my ($lsn,$lag) = @_;
								return if $lsn >= $mlsn;
								return if $lag < $self->{recovery_lag};
								$self->log_error("slave $srv->{t}{server} too slow: $lsn, $lag");
								$srv->{t}->reconnect;
							} else {
								shift;
								$self->log_error( "slave error: @_" );
								$srv->{t}->reconnect(@_);
							}
						});
=cut
					}
				}
			}
		});
	};
}

sub disconnect {
	my $self = shift;
	for my $srv (@{$self->{servers}}) {
		$srv->{t}->disconnect;
	}
}

sub any {
	my $self = shift;
	@{ $self->{stores} } or return undef;
	
	push @{ $self->{stores} }, ( my $one = shift @{ $self->{stores} } );
	return $one;
}

sub master {
	my $self = shift;
	return $self->{master};
}

sub slave {
	my $self = shift;
	return $self->{slave};
}

sub slave_or_master {
	my $self = shift;
	return $self->{slave} || $self->{master};
}


1;
