use inc::Module::Install;
name 'Jonk';
all_from 'lib/Jonk.pm';

requires 'Try::Tiny';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More' => '0.94';

auto_set_repository;

WriteAll;
