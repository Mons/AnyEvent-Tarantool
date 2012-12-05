package AnyEvent::Tarantool::Queue::Task;

sub cb {
	my $self = shift;
	my $cb = shift;
	$self->{cb} = $cb;
}

sub release {
	my $self = shift;
	if (ref $_[0]) {
		 for ( qw( prio delay ttr ttl ) ) {
			if (exists $_[0]{ $_ }) {
				$self->{$_} = $_[0]{ $_ };
			}
		};
	}
	$self->{cb} and return + (delete $self->{cb})->('release');
	warn "Have no callback set for";
}

sub ack {
	my $self = shift;
	$self->{cb} and return + (delete $self->{cb})->('ack');
	warn "Have no callback set for";
}

sub DESTROY {
	my $self = shift;
	$self->{cb} and $self->{cb}('release');
}


package AnyEvent::Tarantool::Queue;

#use uni::perl ':dumper', ':xd';
use 5.008008;
use strict;
use warnings;
no warnings 'uninitialized';
use Carp;
use Scalar::Util 'weaken';
use parent 'AnyEvent::Tarantool';

BEGIN {
	if (eval { require JSON::XS; }) {
		my $JSON = JSON::XS->new->pretty->utf8;
		*JSON = sub () { $JSON };
	}
	else {
		*JSON = sub () { 0 }
	}
}

sub init {
	my $self = shift;
	$self->next::method(@_);
	$self->{space} = {no => delete $self->{space}} unless ref $self->{space};
	$self->{space}{no} ||= 0;
	$self->{space}{unpack} ||= '';
	$self->{take_timeout} ||= 1;
}

sub _on_connreset {
	my $self = shift;
	warn "connreset";
	$self->next::method(@_);
	%{ $self->{watcher} } = ();
}

sub _on_connect_success {
	my ($self,$fh,$host,$port) = @_;
	$self->enable(sub {
		my ($pk,$err,$pkt) = @_;
		if (my $ok = shift or $err =~ /queue already enabled/) {
			$self->{ready} = 1;
			warn "@_";
			$self->{connected} && $self->{connected}->( $self, $host,$port );
			
		}
		else {
			if ($err =~ /Procedure .+ not defined/) {
				warn "Queue lua module not loaded: $err";
			}
			else {
				warn $err;
			}
			if ($self->{reconnect}) {
				$self->{connfail} && $self->{connfail}->( $err );
			} else {
				$self->{disconnected} && $self->{disconnected}->( $err );
			}
			$self->_reconnect_after;
		}
	});
}

sub _handle_packet {
	my ($self,$pkt, $args) = @_;
	if ($args) {
		return if $args->{nohandle};
		if (JSON and $args->{decode}) {
			for (@{ $pkt->{tuples} }) {
				for (@$_) {
					$_ = JSON->decode($_);
				}
			}
			return;
		}
	}
	if ($pkt->{count} and @{ $pkt->{tuples} }) {
		for (@{ $pkt->{tuples} }) {
			my $s = {};my @tail;
			( @$s{ qw( id tube prio status runat ttr ttl taken buried ) }, @tail ) = @$_;
			delete $s->{runat};
			$s->{ttr} /= 1E6;
			if ($s->{ttl} eq "18446744073709551615" ) {
				delete $self->{ttl};
			} else {
				$s->{ttl} = $s->{ttl} / 1E6;
			}
			$s->{data} = \@tail;
			$_ = bless $s, 'AnyEvent::Tarantool::Queue::Task';
		}
	}
}


sub enable {
	my $self = shift;
	my $cb = pop;
	$self->lua('box.queue.enable', [ $self->{space}{no} ], $cb);
}

sub disable {
	my $self = shift;
	my $cb = pop;
	$self->lua('box.queue.disable', [ $self->{space}{no} ], $cb);
}

sub stats { # (tube || 0, timeout || 1.s)
	my $self = shift;
	my $cb = pop;
	warn "call stats";
	$self->lua('box.queue.stats', [ $self->{space}{no} ], { out => 'p', args => { decode => 'json' } } , $cb);
	
}

sub take { # (tube || 0, timeout || 1.s)
	my $self = shift;
	my $cb = pop;
	
	my ($tube,$timeout) = @_;
	$tube ||= 0;
	$timeout ||= $self->{take_timeout};
	$self->lua('box.queue.take', [ $self->{space}{no}, $tube, $timeout ], { out => 'LIIpLLLII'.$self->{space}{unpack} }, $cb);
	
}

sub release { # TODO: method signature
	my $self = shift;
	my $cb; $cb = ref $_[-1] eq 'CODE' ? pop : sub {};
	my $task = ref $_[0] ? shift : { id => shift };
	$self->lua('box.queue.release', [ $self->{space}{no}, $task->{id}, $task->{prio}, $task->{delay}, $task->{ttr}, undef ], { out => 'LIIpLLLII' }, $cb);
}

sub ack {
	my $self = shift;
	my $cb; $cb = ref $_[-1] eq 'CODE' ? pop : sub {};
	my $task = ref $_[0] ? shift : { id => shift };
	$self->lua('box.queue.ack', [ $self->{space}{no}, $task->{id} ], { out => 'LIIpLLLII'.$self->{space}{unpack} }, $cb);
}


sub watch { # ($tube)
	weaken( my $self = shift );
	my $cb   = pop;
	my $no; 
	if ( ref $_[-1] eq 'CODE' ) {
		$no = $cb; $cb = pop;
	}
	my $hv = 0;
	my $tube = shift || 0;
	return $cb->(undef, "Already have watcher for $tube") if exists $self->{_}{watcher}{$tube};
	$self->{_}{watcher}{$tube} = sub {
		if ($_[0] and $_[0]{count}) {
			$hv++;
			#warn dumper $_[0];
			
			# TODO: more than one task
			
			weaken( my $task = $_[0]{tuples}[0] );
			$task->cb( sub {
				
				#warn "cb @_";
				$self and $task or return;
				my $cmd = shift;
				if ($self->can($cmd)) {
					$self->$cmd( $task );
				} else {
					warn "Unknown method from task: $cmd";
				}
				
				exists $self->{_}{watcher}{$tube} or return;
				$self->take($tube, $self->{_}{watcher}{$tube});
			} );
			
			$cb->( $_[0]{tuples}[0] );
		} else {
			$self or return;
			return unless exists $self->{_}{watcher}{$tube};
			$no->() if $hv and $no;
			$hv = 0;
			#warn dumper \@_;
			my $d;$d = AE::timer 0.01,0, sub {
				undef $d;
				$self->take($tube, $self->{_}{watcher}{$tube});
			};
		}
	};
	$self->take($tube, $self->{_}{watcher}{$tube}) for 1..2; # prefetch
	return defined wantarray && guard { $self and delete $self->{_}{watcher}{$tube} }
}

1;

