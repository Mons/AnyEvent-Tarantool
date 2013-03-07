use 5.008008;
use ExtUtils::MakeMaker;

$ENV{AUTHOR} and require CPAN::Meta;

WriteMakefile(
	NAME              => 'AnyEvent::Tarantool',
	VERSION_FROM      => 'lib/AnyEvent/Tarantool.pm',
	PREREQ_PM         => {
		'AnyEvent' => 5,
		'Protocol-Tarantool' => '0.04',
	},
	ABSTRACT_FROM  => 'lib/AnyEvent/Tarantool.pm', # retrieve abstract from module
	AUTHOR         => 'Mons Anderson <mons@cpan.org>',
	LICENSE        => 'perl',
	
);
