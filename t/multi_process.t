use strict;
use warnings;
use t::Utils;
use Test::More;
use Test::SharedFork;
use Jonk;

my $dbh = t::Utils->setup;

    {   # insert test job
        my $jonk = Jonk->new($dbh);
        ok $jonk->insert('MyWorker', 'arg1');
        ok $jonk->insert('MyWorker', 'arg2');
    }

    if ( fork ) {
        my $dbh = t::Utils->setup;

        my $jonk = Jonk->new($dbh, {functions => [qw/MyWorker/]});
        my $job = $jonk->find_job();
        is $job->arg, 'arg1';

        wait;

        $job->completed;
    }
    else {
        # child
        my $dbh = t::Utils->setup;

        sleep 1;

        my $jonk = Jonk->new($dbh, {functions => [qw/MyWorker/]});
        my $job = $jonk->find_job();

        is $job->arg, 'arg2';
        $job->completed;
    }

done_testing;
