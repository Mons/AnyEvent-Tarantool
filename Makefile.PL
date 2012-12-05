use 5.008008;
use ExtUtils::MakeMaker;

our $AUTHOR = -e '.git';
if ($AUTHOR) {
	#system( 'pod2text ... > README' ) and die "$!";
}
unlink 'META.yml';


WriteMakefile(
	NAME              => 'AnyEvent::Tarantool',
	VERSION_FROM      => 'lib/AnyEvent/Tarantool.pm',
	PREREQ_PM         => {
		AnyEvent => 5,
		'Protocol-Tarantool' => '0.02',
	},
	ABSTRACT_FROM  => 'lib/AnyEvent/Tarantool.pm', # retrieve abstract from module
	AUTHOR         => 'Mons Anderson <mons@cpan.org>',
	LICENSE        => 'perl',
	
);

__END__
use inc::Module::Install;

my $MI  = $Module::Install::MAIN;
name           'AnyEvent-Tarantool';
all_from       'lib/AnyEvent/Tarantool.pm';
author         'Mons Anderson <mons@cpan.org>';
license        'perl';

build_requires 'Test::More';

requires 'Protocol::Tarantool';

auto_provides;
auto_install;

WriteAll;

print STDERR "Generated makefile for ".$MI->load('name')->name."-".$MI->load('version')->version."\n";
