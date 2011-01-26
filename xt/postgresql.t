use strict;
use warnings;
use xt::Utils::postgresql;
use Test::More;

subtest 'client'        => sub { do "t/client.t"        };
subtest 'worker'        => sub { do "t/worker.t"        };
subtest 'job'           => sub { do "t/job.t"           };
subtest 'multi_process' => sub { do "t/multi_process.t" };

done_testing;
