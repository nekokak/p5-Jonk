use inc::Module::Install;
name 'Jonk';
all_from 'lib/Jonk.pm';

requires 'Try::Tiny';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More' => '0.94';
test_requires 'Module::Install::TestTarget' => '0.14';
test_requires 'Test::mysqld' => '0.11';

default_test_target
    run_on_prepare => ['t/start_mysqld.pl'],
;

auto_set_repository;

WriteAll;
